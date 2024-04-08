import { InvalidRequestError } from '@atproto/xrpc-server'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import { getAlgos } from '../algos'
import { AtUri } from '@atproto/syntax'
import { neon } from '@neondatabase/serverless'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    const feedUri = new AtUri(params.feed)
    const sql = neon(ctx.cfg.handlesDatabase)
    const algos = await getAlgos(sql)
    const algo = algos.find((a) => a.shortName === feedUri.rkey)
    if (
      feedUri.hostname !== ctx.cfg.publisherDid ||
      feedUri.collection !== 'app.bsky.feed.generator' ||
      !algo
    ) {
      throw new InvalidRequestError(
        'Unsupported algorithm',
        'UnsupportedAlgorithm',
      )
    }
    /**
     * Example of how to check auth if giving user-specific results:
     *
     * const requesterDid = await validateAuth(
     *   req,
     *   ctx.cfg.serviceDid,
     *   ctx.didResolver,
     * )
     */

    const body = await algo.handler(ctx, params)
    return {
      encoding: 'application/json',
      body: body,
    }
  })
}
