import dotenv from 'dotenv'
import FeedGenerator from './server'
import cron from 'node-cron'

import { fetch } from 'undici'
import { neon } from '@neondatabase/serverless'
// @ts-expect-error
globalThis.fetch = fetch

const run = async () => {
  dotenv.config()
  const hostname = maybeStr(process.env.FEEDGEN_HOSTNAME) ?? 'example.com'
  const serviceDid =
    maybeStr(process.env.FEEDGEN_SERVICE_DID) ?? `did:web:${hostname}`
  const server = FeedGenerator.create({
    port: maybeInt(process.env.FEEDGEN_PORT) ?? 3000,
    listenhost: maybeStr(process.env.FEEDGEN_LISTENHOST) ?? 'localhost',
    sqliteLocation: maybeStr(process.env.FEEDGEN_SQLITE_LOCATION) ?? ':memory:',
    subscriptionEndpoint:
      maybeStr(process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT) ??
      'wss://bsky.social',
    publisherDid:
      maybeStr(process.env.FEEDGEN_PUBLISHER_DID) ?? 'did:example:alice',
    subscriptionReconnectDelay:
      maybeInt(process.env.FEEDGEN_SUBSCRIPTION_RECONNECT_DELAY) ?? 3000,
    hostname,
    serviceDid,
    handlesDatabase: definitelyStr(process.env.HANDLES_DATABASE),
  })
  await server.start()
  console.log(
    `🤖 running feed generator at http://${server.cfg.listenhost}:${server.cfg.port}`,
  )

  // every 30 minutes, limit each domain to 1000 posts
  cron.schedule('*/30 * * * *', async () => {
    const sql = neon(server.cfg.handlesDatabase)

    const res = await sql('SELECT name FROM "Domain"')
    const domains = res
      .map(({ name }) => name as string)
      .filter((name) => name !== 'localhost')

    for (const domain of domains) {
      const posts = await sql(
        'SELECT uri FROM "Post" WHERE domain = ? ORDER BY "indexedAt" DESC LIMIT 1000',
        [domain],
      )
      const uris = posts.map(({ uri }) => uri as string)
      if (uris.length === 0) continue
      await sql('DELETE FROM "Post" WHERE domain = ? AND uri NOT IN (?)', [
        domain,
        uris,
      ])
    }
  })
}

const maybeStr = (val?: string) => {
  if (!val) return undefined
  return val
}

const definitelyStr = (val?: string) => {
  if (!val) throw new Error('missing required env var')
  return val
}

const maybeInt = (val?: string) => {
  if (!val) return undefined
  const int = parseInt(val, 10)
  if (isNaN(int)) return undefined
  return int
}

run()
