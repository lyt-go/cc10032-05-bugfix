import { fetchJson, request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
  basis: Record<string, unknown> = {},
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action, ...(Object.keys(basis).length ? { basis } : {}) }),
  })
  return await response.json()
}

export type LateSideOutputRow = {
  event_id: string
  event_ts: number | null
  delay: number | null
  reason: string
  confirmed_at: number | null
  confirmation_version: number | null
}

export type LateSideOutputResult = {
  ok: boolean
  total: number
  items: LateSideOutputRow[]
}

export type LateEventRow = {
  event_id: string
  event_ts: number | null
  classification: string
  delay: number | null
  reason: string
  original_classification?: Record<string, unknown> | null
  classification_history?: Record<string, unknown>[]
  confirmation: { offset: number | null; version: number | null; confirmed: boolean }
}

export type LateEventResult = {
  ok: boolean
  total: number
  items: LateEventRow[]
}

export type LateRejudgeResult = {
  ok: boolean
  rejudge: {
    rejudge_id: number
    status: string
    basis: { confirmed_at: number | null; requested_by: string; requested_at: number; note: string }
    result?: {
      time_basis: number | null
      confirmation_version: number | null
      side_output_added: string[]
      side_output_removed: string[]
      by_event: Record<string, unknown>[]
    } | null
  } | null
}

export async function loadLateSideOutput(id: number): Promise<LateSideOutputResult> {
  return await fetchJson<LateSideOutputResult>(`/api/late_data/${id}/side-output`)
}

export async function loadLateEvents(id: number): Promise<LateEventResult> {
  return await fetchJson<LateEventResult>(`/api/late_data/${id}/events`)
}

export async function loadLateRejudge(id: number): Promise<LateRejudgeResult> {
  return await fetchJson<LateRejudgeResult>(`/api/late_data/${id}/rejudge`)
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
