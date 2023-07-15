import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { neon } from '@neondatabase/serverless'

type User = {
  did: string
  handle: string
  domain: string
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  sql: ReturnType<typeof neon>
  users: User[] = []
  lastFetched = 0

  constructor(a, b, connectionString: string) {
    super(a, b)
    this.sql = neon(connectionString)
    this.fetchDomains()
  }

  async fetchDomains() {
    this.lastFetched = Date.now()
    const users = await this.sql(
      'SELECT did, handle, "Domain".name AS "domain" FROM "User" JOIN "Domain" ON "User"."domainId" = "Domain".id',
    )
    this.users = Object.values(users) as User[]
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
