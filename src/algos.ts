import { neon } from '@neondatabase/serverless'

import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from './lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from './config'
import { InvalidRequestError } from '@atproto/xrpc-server'

type AlgoHandler = (ctx: AppContext, params: QueryParams) => Promise<AlgoOutput>

let domains: string[] = []
let lastUpdated = 0

export async function getAlgos(
  connectionString: string,
): Promise<{ shortName: string; handler: AlgoHandler }[]> {
  const sql = neon(connectionString)

  if (Date.now() - lastUpdated > 1000 * 60 * 60) {
    lastUpdated = Date.now()
    const res = await sql('SELECT name FROM "Domain"')
    domains = res
      .map(({ name }) => name as string)
      .filter((name) => name !== 'localhost')
  }

  return domains.map((domain) => ({
    shortName: domain,
    handler: async (ctx, params) => {
      let builder = ctx.db
        .selectFrom('post')
        .selectAll()
        .where('post.domain', '=', domain)
        .orderBy('indexedAt', 'desc')
        .orderBy('cid', 'desc')
        .limit(params.limit)

      if (params.cursor) {
        const [indexedAt, cid] = params.cursor.split('::')
        if (!indexedAt || !cid) {
          throw new InvalidRequestError('malformed cursor')
        }
        const timeStr = new Date(parseInt(indexedAt, 10)).toISOString()
        builder = builder
          .where('post.indexedAt', '<', timeStr)
          .orWhere((qb) => qb.where('post.indexedAt', '=', timeStr))
          .where('post.cid', '<', cid)
      }
      const res = await builder.execute()

      const feed = res.map((row) => ({
        post: row.uri,
      }))

      let cursor: string | undefined
      const last = res.at(-1)
      if (last) {
        cursor = `${new Date(last.indexedAt).getTime()}::${last.cid}`
      }

      return {
        cursor,
        feed,
      }
    },
  }))
}
