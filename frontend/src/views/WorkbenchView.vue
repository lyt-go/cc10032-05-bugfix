<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section v-if="isLateData" class="late-panel" data-testid="late-panel">
      <header class="late-panel-head">
        <h3>迟到判定与侧输出核对</h3>
        <label class="filter-item">
          <span>策略 id</span>
          <input v-model="latePolicyId" data-testid="late-policy-id" placeholder="选择左侧策略 id" />
        </label>
        <label class="filter-item">
          <span>重判时间依据</span>
          <input v-model="rejudgeBasis" data-testid="late-rejudge-basis" placeholder="发起重判所选确认时刻" />
        </label>
        <button class="btn" data-testid="late-refresh" type="button" @click="refreshLate">刷新</button>
      </header>
      <p v-if="lateError" class="error-text">{{ lateError }}</p>
      <div class="late-columns">
        <article>
          <h4>迟到侧输出（共 {{ sideOutput.length }} 行）</h4>
          <table class="data-table">
            <thead><tr><th>事件</th><th>事件时刻</th><th>延迟</th><th>确认时刻</th><th>确认版本</th></tr></thead>
            <tbody>
              <tr v-for="item in sideOutput" :key="item.event_id" :data-testid="`side-${item.event_id}`">
                <td>{{ item.event_id }}</td>
                <td>{{ item.event_ts ?? '—' }}</td>
                <td>{{ item.delay ?? '—' }}</td>
                <td>{{ item.confirmed_at ?? '—' }}</td>
                <td>{{ item.confirmation_version ?? '—' }}</td>
              </tr>
              <tr v-if="!sideOutput.length"><td colspan="5" class="empty-state">暂无侧输出行</td></tr>
            </tbody>
          </table>
        </article>
        <article>
          <h4>分类结果与旧分类</h4>
          <table class="data-table">
            <thead><tr><th>事件</th><th>当前分类</th><th>延迟</th><th>旧分类</th><th>说明</th></tr></thead>
            <tbody>
              <tr v-for="item in lateEvents" :key="item.event_id">
                <td>{{ item.event_id }}</td>
                <td>{{ item.classification }}</td>
                <td>{{ item.delay ?? '—' }}</td>
                <td>{{ item.original_classification?.classification ?? '—' }}</td>
                <td>{{ item.reason }}</td>
              </tr>
              <tr v-if="!lateEvents.length"><td colspan="5" class="empty-state">暂无分类结果</td></tr>
            </tbody>
          </table>
        </article>
      </div>
      <article v-if="rejudge" class="late-rejudge">
        <h4>迟到重判 #{{ rejudge.rejudge_id }}（{{ rejudge.status }}）</h4>
        <p>时间依据：{{ rejudge.basis.confirmed_at ?? '—' }}，发起人：{{ rejudge.basis.requested_by }}</p>
        <p v-if="rejudge.result">
          重判引用时刻：{{ rejudge.result.time_basis ?? '—' }}，
          确认版本：{{ rejudge.result.confirmation_version ?? '—' }}，
          侧输出新增 {{ rejudge.result.side_output_added.join('、') || '无' }}、
          移除 {{ rejudge.result.side_output_removed.join('、') || '无' }}
        </p>
      </article>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadLateEvents,
  loadLateRejudge,
  loadLateSideOutput,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import type { LateEventRow, LateRejudgeResult, LateSideOutputRow } from '@/api/backend-service'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

const isLateData = computed(() => meta.key === 'late_data')
const latePolicyId = ref('')
const sideOutput = ref<LateSideOutputRow[]>([])
const lateEvents = ref<LateEventRow[]>([])
const rejudge = ref<LateRejudgeResult['rejudge']>(null)
const rejudgeBasis = ref('')
const lateError = ref('')

async function refreshLate() {
  if (!isLateData.value) return
  const id = Number(latePolicyId.value || rows.value[0]?.id)
  if (!id) return
  if (!latePolicyId.value) latePolicyId.value = String(id)
  lateError.value = ''
  try {
    const [side, events, judge] = await Promise.all([
      loadLateSideOutput(id),
      loadLateEvents(id),
      loadLateRejudge(id),
    ])
    sideOutput.value = side.items
    lateEvents.value = events.items
    rejudge.value = judge.rejudge
  } catch (error) {
    lateError.value = error instanceof Error ? error.message : '迟到判定结果读取失败'
  }
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const id = Number(row.id)
    // 发起迟到重判时必须带上值班员选择的确认时刻依据；其余动作原样下发。
    const basis = action === '发起迟到重判' && rejudgeBasis.value
      ? { confirmed_at: Number(rejudgeBasis.value) }
      : {}
    const result = await applyAction(meta.key, id, action, basis)
    if (!result.ok) throw new Error(result.message)
    await reload()
    if (isLateData.value) {
      latePolicyId.value = String(id)
      await refreshLate()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    if (isLateData.value && !latePolicyId.value && rows.value.length) {
      latePolicyId.value = String(rows.value[0].id)
      await refreshLate()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
