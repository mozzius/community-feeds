import { Server } from '../lexicon'
import { AppContext } from '../config'
import { getAlgos } from '../algos'
import { AtUri } from '@atproto/uri'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.describeFeedGenerator(async () => {
    const algos = await getAlgos(ctx.cfg.handlesDatabase)
    const feeds = algos.map(({ shortName }) => ({
      uri: AtUri.make(
        ctx.cfg.publisherDid,
        'app.bsky.feed.generator',
        shortName,
      ).toString(),
    }))
    return {
      encoding: 'application/json',
      body: {
        did: ctx.cfg.serviceDid,
        feeds,
      },
    }
  })
}
