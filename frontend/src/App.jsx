import { useState, useEffect } from 'react'
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar, ComposedChart,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell
} from 'recharts'

// Metric Card Component
function MetricCard({ title, value, subtitle, trend, icon }) {
  const isPositive = trend >= 0
  return (
    <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
      <div className="flex items-center justify-between mb-2">
        <span className="text-gray-500 text-sm font-medium">{title}</span>
        <span className="text-2xl">{icon}</span>
      </div>
      <div className="text-3xl font-bold text-gray-900 mb-1">{value}</div>
      {subtitle && <div className="text-sm text-gray-500">{subtitle}</div>}
      {trend !== undefined && (
        <div className={`text-sm mt-2 ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
          {isPositive ? '↑' : '↓'} {Math.abs(trend).toFixed(1)}% from last month
        </div>
      )}
    </div>
  )
}

// Format currency
const formatCurrency = (value) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

// Colors
const COLORS = {
  primary: '#635BFF',
  success: '#30D158',
  danger: '#FF3B30',
  warning: '#FF9500',
  info: '#007AFF',
  gray: '#8E8E93'
}

const PIE_COLORS = [COLORS.success, COLORS.danger, COLORS.warning]

function App() {
  const [mrrData, setMrrData] = useState([])
  const [summary, setSummary] = useState(null)
  const [subscriptions, setSubscriptions] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function fetchData() {
      try {
        const [mrrRes, summaryRes, subsRes] = await Promise.all([
          fetch('/api/mrr-trend'),
          fetch('/api/summary'),
          fetch('/api/subscriptions')
        ])

        if (!mrrRes.ok || !summaryRes.ok || !subsRes.ok) {
          throw new Error('Failed to fetch data')
        }

        const [mrrJson, summaryJson, subsJson] = await Promise.all([
          mrrRes.json(),
          summaryRes.json(),
          subsRes.json()
        ])

        setMrrData(mrrJson)
        setSummary(summaryJson)
        setSubscriptions(subsJson)
        setLoading(false)
      } catch (err) {
        setError(err.message)
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-xl text-gray-600">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="text-xl text-red-600 mb-4">Error: {error}</div>
          <div className="text-gray-500">Make sure the API server is running on port 5001</div>
        </div>
      </div>
    )
  }

  const pieData = subscriptions.map(s => ({
    name: s.status.charAt(0).toUpperCase() + s.status.slice(1).replace('_', ' '),
    value: s.count
  }))

  return (
    <div className="min-h-screen p-8">
      {/* Header */}
      <header className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 flex items-center gap-3">
          <span className="text-4xl">📊</span>
          Stripe MRR Analytics
        </h1>
        <p className="text-gray-500 mt-1">Real-time revenue metrics from BigQuery</p>
      </header>

      {/* Metric Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <MetricCard
          title="Current MRR"
          value={formatCurrency(summary?.currentMrr || 0)}
          trend={summary?.growthRate}
          icon="💰"
        />
        <MetricCard
          title="Active Customers"
          value={summary?.activeCustomers || 0}
          subtitle={`${summary?.subscriptions?.pastDue || 0} past due`}
          icon="👥"
        />
        <MetricCard
          title="ARPU"
          value={formatCurrency(summary?.arpu || 0)}
          subtitle="Average Revenue Per User"
          icon="📈"
        />
        <MetricCard
          title="Churn Rate"
          value={`${(summary?.churnRate || 0).toFixed(1)}%`}
          subtitle={`${summary?.subscriptions?.canceled || 0} canceled`}
          icon="📉"
        />
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* MRR Trend Chart with Active Subscriptions */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">MRR Trend & Active Subscriptions</h2>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={mrrData}>
              <defs>
                <linearGradient id="colorMrr" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={COLORS.primary} stopOpacity={0.3}/>
                  <stop offset="95%" stopColor={COLORS.primary} stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis 
                yAxisId="mrr"
                tickFormatter={(v) => `$${v/1000}k`} 
                tick={{ fontSize: 12 }} 
                orientation="left"
              />
              <YAxis 
                yAxisId="customers"
                tick={{ fontSize: 12 }} 
                orientation="right"
                tickFormatter={(v) => `${v}`}
              />
              <Tooltip 
                formatter={(value, name) => {
                  if (name === 'Active Subscriptions') return [value, name]
                  return [formatCurrency(value), 'MRR']
                }}
                contentStyle={{ borderRadius: 8 }}
              />
              <Legend />
              <Bar 
                yAxisId="customers"
                dataKey="activeCustomers" 
                name="Active Subscriptions" 
                fill={COLORS.info} 
                opacity={0.6}
                radius={[4, 4, 0, 0]} 
              />
              <Area
                yAxisId="mrr"
                type="monotone"
                dataKey="totalMrr"
                name="MRR"
                stroke={COLORS.primary}
                strokeWidth={3}
                fill="url(#colorMrr)"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* New vs Churned MRR */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">New vs Churned MRR</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={mrrData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis tickFormatter={(v) => `$${v}`} tick={{ fontSize: 12 }} />
              <Tooltip 
                formatter={(value) => formatCurrency(value)}
                contentStyle={{ borderRadius: 8 }}
              />
              <Legend />
              <Bar dataKey="newMrr" name="New MRR" fill={COLORS.success} radius={[4, 4, 0, 0]} />
              <Bar dataKey="churnedMrr" name="Churned MRR" fill={COLORS.danger} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Customer Growth */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100 lg:col-span-2">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Customer Growth</h2>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={mrrData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip contentStyle={{ borderRadius: 8 }} />
              <Legend />
              <Line
                type="monotone"
                dataKey="activeCustomers"
                name="Active Customers"
                stroke={COLORS.info}
                strokeWidth={3}
                dot={{ r: 5 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Subscription Status Pie */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Subscription Status</h2>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={80}
                paddingAngle={5}
                dataKey="value"
                label={({ name, value }) => `${name}: ${value}`}
              >
                {pieData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Data Table */}
      <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Monthly Breakdown</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-3 px-4 text-gray-600 font-medium">Month</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">Total MRR</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">New MRR</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">Churned MRR</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">Net New</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">Customers</th>
                <th className="text-right py-3 px-4 text-gray-600 font-medium">Growth</th>
              </tr>
            </thead>
            <tbody>
              {mrrData.map((row, i) => (
                <tr key={i} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium">{row.month}</td>
                  <td className="py-3 px-4 text-right">{formatCurrency(row.totalMrr)}</td>
                  <td className="py-3 px-4 text-right text-green-600">+{formatCurrency(row.newMrr)}</td>
                  <td className="py-3 px-4 text-right text-red-600">-{formatCurrency(row.churnedMrr)}</td>
                  <td className="py-3 px-4 text-right">{formatCurrency(row.netNewMrr)}</td>
                  <td className="py-3 px-4 text-right">{row.activeCustomers}</td>
                  <td className={`py-3 px-4 text-right ${row.growthRate >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {row.growthRate ? `${row.growthRate >= 0 ? '+' : ''}${row.growthRate.toFixed(1)}%` : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Footer */}
      <footer className="mt-8 text-center text-gray-400 text-sm">
        Data sourced from Stripe via BigQuery • Last updated: {new Date().toLocaleDateString()}
      </footer>
    </div>
  )
}

export default App
