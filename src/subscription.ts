import { BskyAgent } from '@atproto/api'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { neon } from '@neondatabase/serverless'
import cron from 'node-cron'

type User = {
  did: string
  handle: string
  domain: string
}

const agent = new BskyAgent({
  service: 'https://public.api.bsky.app',
})

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  sql: ReturnType<typeof neon>
  users: User[] = []
  lastFetched = 0

  constructor(a, b, connectionString: string) {
    super(a, b)
    this.sql = neon(connectionString)
    this.fetchDomains()

    cron.schedule('*/15 * * * *', async () => {
      try {
        await this.fetchDomains()
      } catch (e) {
        console.log('error fetching domains')
        console.error(e)
      }
    })
  }

  async fetchDomains() {
    this.lastFetched = Date.now()
    const users = (await this.sql(
      'SELECT did, handle, "Domain".name AS "domain" FROM "User" JOIN "Domain" ON "User"."domainId" = "Domain".id',
    )) as User[]

    // split
    const userChunks = users.reduce<User[][]>(
      (acc, subject) => {
        if (acc[acc.length - 1]!.length === 25) {
          acc.push([subject])
        } else {
          acc[acc.length - 1]!.push(subject)
        }
        return acc
      },
      [[]],
    )

    const validUsers = new Set<User>()

    for (const chunk of userChunks) {
      const {
        data: { profiles },
      } = await agent.getProfiles({
        actors: chunk.map((user) => user.did),
      })

      for (const profile of profiles) {
        const user = chunk.find((user) => user.did === profile.did)
        if (!user) continue
        if (profile.handle.endsWith(user.domain)) {
          validUsers.add(user)
        }
      }
    }

    this.users = Array.from(validUsers)

    console.log(users.length, this.users.length)
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    if (this.lastFetched < Date.now() - 1000 * 60 * 60) {
      this.fetchDomains()
    }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        return this.users.some((user) => user.did === create.author)
      })
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          domain: this.users.find((user) => user.did === create.author)!.domain,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
