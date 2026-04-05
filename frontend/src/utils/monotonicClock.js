export function advanceMonotonicNowUs(nowUsRef, nowUsWallRef, packetNowUs) {
  const wallNowMs = performance.now()
  const safePacketNowUs = Number(packetNowUs ?? 0)
  const projectedNowUs = nowUsRef.current > 0
    ? nowUsRef.current + Math.max(0, wallNowMs - nowUsWallRef.current) * 1000
    : safePacketNowUs
  nowUsRef.current = Math.max(projectedNowUs, safePacketNowUs)
  nowUsWallRef.current = wallNowMs
}

export function getMonotonicRenderNowUs(nowUsRef, nowUsWallRef, holdbackUs) {
  const wallNowMs = performance.now()
  const projectedNowUs = Math.max(0, nowUsRef.current + Math.max(0, wallNowMs - nowUsWallRef.current) * 1000)
  return Math.max(0, projectedNowUs - holdbackUs)
}
