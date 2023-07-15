import dotenv from 'dotenv'
import { AtpAgent, BlobRef } from '@atproto/api'
import fs from 'fs/promises'
import { ids } from '../src/lexicon/lexicons'
import { neon } from '@neondatabase/serverless'

const run = async () => {
  dotenv.config()

  const sql = neon(process.env.HANDLES_DATABASE!)

  const res = await sql('SELECT name FROM "Domain"')
  const domains = res
    .map(({ name }) => name as string)
    .filter((name) => name !== 'localhost')

  // YOUR bluesky handle
  // Ex: user.bsky.social
  const handle = process.env.BSKY_HANDLE!

  // YOUR bluesky password, or preferably an App Password (found in your client settings)
  // Ex: abcd-1234-efgh-5678
  const password = process.env.BSKY_PASSWORD!

  for (const domain of domains) {
    // A short name for the record that will show in urls
    // Lowercase with no spaces.
    // Ex: whats-hot
    const recordName = domain

    // A display name for your feed
    // Ex: What's Hot
    const displayName = `${domain} handles`

    // (Optional) A description of your feed
    // Ex: Top trending content from the whole network
    const description = `All posts from the ${domain} community.\n\nGet your own ${domain} handle at https://${domain} to be featured on the feed!`

    // (Optional) The path to an image to be used as your feed's avatar
    // Ex: ~/path/to/avatar.jpeg
    const avatar =
      '/Users/samuel/Documents/Programming/community-feeds/at-sign.png'

    // -------------------------------------
    // NO NEED TO TOUCH ANYTHING BELOW HERE
    // -------------------------------------

    if (!process.env.FEEDGEN_SERVICE_DID && !process.env.FEEDGEN_HOSTNAME) {
      throw new Error('Please provide a hostname in the .env file')
    }
    const feedGenDid =
      process.env.FEEDGEN_SERVICE_DID ??
      `did:web:${process.env.FEEDGEN_HOSTNAME}`

    // only update this if in a test environment
    const agent = new AtpAgent({ service: 'https://bsky.social' })
    await agent.login({ identifier: handle, password })

    try {
      await agent.api.app.bsky.feed.describeFeedGenerator()
    } catch (err) {
      throw new Error(
        'The bluesky server is not ready to accept published custom feeds yet',
      )
    }

    let avatarRef: BlobRef | undefined
    if (avatar) {
      let encoding: string
      if (avatar.endsWith('png')) {
        encoding = 'image/png'
      } else if (avatar.endsWith('jpg') || avatar.endsWith('jpeg')) {
        encoding = 'image/jpeg'
      } else {
        throw new Error('expected png or jpeg')
      }
      const img = await fs.readFile(avatar)
      const blobRes = await agent.api.com.atproto.repo.uploadBlob(img, {
        encoding,
      })
      avatarRef = blobRes.data.blob
    }

    await agent.api.com.atproto.repo.putRecord({
      repo: agent.session?.did ?? '',
      collection: ids.AppBskyFeedGenerator,
      rkey: recordName,
      record: {
        did: feedGenDid,
        displayName: displayName,
        description: description,
        avatar: avatarRef,
        createdAt: new Date().toISOString(),
      },
    })

    console.log(`Published feed generator for ${domain}`)
  }

  console.log('All done 🎉')
}

run()
