export type DatabaseSchema = {
  post: Post
  sub_state: SubState
}

export type Post = {
  uri: string
  cid: string
  domain: string
  replyParent: string | null
  replyRoot: string | null
  indexedAt: string
}

export type SubState = {
  service: string
  cursor: number
}
