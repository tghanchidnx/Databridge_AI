import { useEffect, useRef, useState } from 'react'
import { MagnifyingGlassPlus, MagnifyingGlassMinus, ArrowsOut } from "@phosphor-icons/react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import type { SchemaComparison, DependencyNode } from '@/types'

interface DependencyGraphProps {
  comparison: SchemaComparison
}

export function DependencyGraph({ comparison }: DependencyGraphProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [layout, setLayout] = useState<'hierarchical' | 'force' | 'circular'>('hierarchical')
  const [zoom, setZoom] = useState(1)
  const [selectedNode, setSelectedNode] = useState<DependencyNode | null>(null)

  const mockDependencies: DependencyNode[] = [
    {
      objectId: 'customers',
      objectName: 'customers',
      objectType: 'table',
      dependsOn: [],
      referencedBy: [
        { name: 'orders', type: 'table' },
        { name: 'vw_active_customers', type: 'view' }
      ],
      dependencyLevel: 0,
      deploymentOrder: 1
    },
    {
      objectId: 'orders',
      objectName: 'orders',
      objectType: 'table',
      dependsOn: [
        { name: 'customers', type: 'table' }
      ],
      referencedBy: [
        { name: 'vw_order_summary', type: 'view' }
      ],
      dependencyLevel: 1,
      deploymentOrder: 2
    },
    {
      objectId: 'vw_active_customers',
      objectName: 'vw_active_customers',
      objectType: 'view',
      dependsOn: [
        { name: 'customers', type: 'table' }
      ],
      referencedBy: [],
      dependencyLevel: 1,
      deploymentOrder: 3
    },
    {
      objectId: 'vw_order_summary',
      objectName: 'vw_order_summary',
      objectType: 'view',
      dependsOn: [
        { name: 'orders', type: 'table' },
        { name: 'customers', type: 'table' }
      ],
      referencedBy: [],
      dependencyLevel: 2,
      deploymentOrder: 4
    }
  ]

  useEffect(() => {
    drawGraph()
  }, [layout, zoom])

  const drawGraph = () => {
    const canvas = canvasRef.current
    if (!canvas) return

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    canvas.width = canvas.offsetWidth
    canvas.height = canvas.offsetHeight

    ctx.clearRect(0, 0, canvas.width, canvas.height)
    ctx.save()
    ctx.scale(zoom, zoom)

    const centerX = canvas.width / (2 * zoom)
    const centerY = canvas.height / (2 * zoom)
    
    if (layout === 'hierarchical') {
      drawHierarchicalLayout(ctx, mockDependencies, centerX, centerY)
    } else if (layout === 'circular') {
      drawCircularLayout(ctx, mockDependencies, centerX, centerY)
    } else {
      drawForceLayout(ctx, mockDependencies, centerX, centerY)
    }

    ctx.restore()
  }

  const drawHierarchicalLayout = (ctx: CanvasRenderingContext2D, nodes: DependencyNode[], centerX: number, centerY: number) => {
    const levelHeight = 100
    const nodeSpacing = 150
    const startY = 50

    const levelGroups = nodes.reduce((acc, node) => {
      const level = node.dependencyLevel
      if (!acc[level]) acc[level] = []
      acc[level].push(node)
      return acc
    }, {} as Record<number, DependencyNode[]>)

    const nodePositions = new Map<string, { x: number; y: number }>()

    Object.entries(levelGroups).forEach(([level, levelNodes]) => {
      const y = startY + parseInt(level) * levelHeight
      const totalWidth = (levelNodes.length - 1) * nodeSpacing
      const startX = centerX - totalWidth / 2

      levelNodes.forEach((node, idx) => {
        const x = startX + idx * nodeSpacing
        nodePositions.set(node.objectId, { x, y })
      })
    })

    ctx.strokeStyle = '#4a5568'
    ctx.lineWidth = 2

    nodes.forEach(node => {
      const nodePos = nodePositions.get(node.objectId)
      if (!nodePos) return

      node.dependsOn.forEach(dep => {
        const depPos = nodePositions.get(dep.name)
        if (!depPos) return

        ctx.beginPath()
        ctx.moveTo(nodePos.x, nodePos.y)
        ctx.lineTo(depPos.x, depPos.y + 25)
        ctx.stroke()

        const angle = Math.atan2(depPos.y + 25 - nodePos.y, depPos.x - nodePos.x)
        ctx.beginPath()
        ctx.moveTo(depPos.x, depPos.y + 25)
        ctx.lineTo(
          depPos.x - 10 * Math.cos(angle - Math.PI / 6),
          depPos.y + 25 - 10 * Math.sin(angle - Math.PI / 6)
        )
        ctx.lineTo(
          depPos.x - 10 * Math.cos(angle + Math.PI / 6),
          depPos.y + 25 - 10 * Math.sin(angle + Math.PI / 6)
        )
        ctx.closePath()
        ctx.fillStyle = '#4a5568'
        ctx.fill()
      })
    })

    nodes.forEach(node => {
      const pos = nodePositions.get(node.objectId)
      if (!pos) return

      const color = node.objectType === 'table' ? '#3b82f6' : '#10b981'
      
      ctx.fillStyle = color
      ctx.beginPath()
      ctx.arc(pos.x, pos.y, 25, 0, Math.PI * 2)
      ctx.fill()

      ctx.fillStyle = '#ffffff'
      ctx.font = 'bold 12px Inter'
      ctx.textAlign = 'center'
      ctx.textBaseline = 'middle'
      
      const typeLabel = node.objectType.charAt(0).toUpperCase()
      ctx.fillText(typeLabel, pos.x, pos.y)

      ctx.fillStyle = '#e2e8f0'
      ctx.font = '11px Inter'
      ctx.fillText(node.objectName, pos.x, pos.y + 40)
    })
  }

  const drawCircularLayout = (ctx: CanvasRenderingContext2D, nodes: DependencyNode[], centerX: number, centerY: number) => {
    const radius = 150
    const nodePositions = new Map<string, { x: number; y: number }>()

    nodes.forEach((node, idx) => {
      const angle = (idx / nodes.length) * 2 * Math.PI - Math.PI / 2
      const x = centerX + radius * Math.cos(angle)
      const y = centerY + radius * Math.sin(angle)
      nodePositions.set(node.objectId, { x, y })
    })

    ctx.strokeStyle = '#4a5568'
    ctx.lineWidth = 1

    nodes.forEach(node => {
      const nodePos = nodePositions.get(node.objectId)
      if (!nodePos) return

      node.dependsOn.forEach(dep => {
        const depPos = nodePositions.get(dep.name)
        if (!depPos) return

        ctx.beginPath()
        ctx.moveTo(nodePos.x, nodePos.y)
        ctx.lineTo(depPos.x, depPos.y)
        ctx.stroke()
      })
    })

    nodes.forEach(node => {
      const pos = nodePositions.get(node.objectId)
      if (!pos) return

      const color = node.objectType === 'table' ? '#3b82f6' : '#10b981'
      
      ctx.fillStyle = color
      ctx.beginPath()
      ctx.arc(pos.x, pos.y, 20, 0, Math.PI * 2)
      ctx.fill()

      ctx.fillStyle = '#ffffff'
      ctx.font = 'bold 11px Inter'
      ctx.textAlign = 'center'
      ctx.textBaseline = 'middle'
      
      const typeLabel = node.objectType.charAt(0).toUpperCase()
      ctx.fillText(typeLabel, pos.x, pos.y)

      ctx.fillStyle = '#e2e8f0'
      ctx.font = '10px Inter'
      ctx.fillText(node.objectName, pos.x, pos.y + 35)
    })
  }

  const drawForceLayout = (ctx: CanvasRenderingContext2D, nodes: DependencyNode[], centerX: number, centerY: number) => {
    drawHierarchicalLayout(ctx, nodes, centerX, centerY)
  }

  return (
    <div className="space-y-6">
      <Card className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="text-lg font-semibold">Dependency Graph</h3>
            <p className="text-sm text-muted-foreground mt-1">
              Visualize object dependencies and deployment order
            </p>
          </div>
          
          <div className="flex items-center gap-2">
            <Select value={layout} onValueChange={(v: any) => setLayout(v)}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="hierarchical">Hierarchical</SelectItem>
                <SelectItem value="force">Force-Directed</SelectItem>
                <SelectItem value="circular">Circular</SelectItem>
              </SelectContent>
            </Select>
            
            <Button
              variant="outline"
              size="icon"
              onClick={() => setZoom(Math.max(0.5, zoom - 0.1))}
            >
              <MagnifyingGlassMinus className="h-4 w-4" />
            </Button>
            
            <Button
              variant="outline"
              size="icon"
              onClick={() => setZoom(Math.min(2, zoom + 0.1))}
            >
              <MagnifyingGlassPlus className="h-4 w-4" />
            </Button>
            
            <Button
              variant="outline"
              size="icon"
              onClick={() => setZoom(1)}
            >
              <ArrowsOut className="h-4 w-4" />
            </Button>
          </div>
        </div>

        <div className="grid grid-cols-4 gap-3 mb-6">
          <Card className="p-3 flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-blue-500 flex items-center justify-center text-white font-bold">
              T
            </div>
            <div>
              <div className="text-xs text-muted-foreground">Tables</div>
              <div className="text-sm font-semibold">Blue Nodes</div>
            </div>
          </Card>
          
          <Card className="p-3 flex items-center gap-3">
            <div className="h-8 w-8 rounded-full bg-green-500 flex items-center justify-center text-white font-bold">
              V
            </div>
            <div>
              <div className="text-xs text-muted-foreground">Views</div>
              <div className="text-sm font-semibold">Green Nodes</div>
            </div>
          </Card>
          
          <Card className="p-3 flex items-center gap-3">
            <div className="h-1 w-8 bg-gray-500" />
            <div>
              <div className="text-xs text-muted-foreground">Dependencies</div>
              <div className="text-sm font-semibold">Arrows</div>
            </div>
          </Card>
          
          <Card className="p-3 flex items-center gap-3">
            <div className="text-xl font-bold text-primary">1→4</div>
            <div>
              <div className="text-xs text-muted-foreground">Deploy Order</div>
              <div className="text-sm font-semibold">By Level</div>
            </div>
          </Card>
        </div>

        <Card className="bg-muted/30">
          <canvas
            ref={canvasRef}
            className="w-full h-[500px] cursor-move"
            onClick={(e) => {
              const rect = e.currentTarget.getBoundingClientRect()
              const x = (e.clientX - rect.left) / zoom
              const y = (e.clientY - rect.top) / zoom
            }}
          />
        </Card>
      </Card>

      {selectedNode && (
        <Card className="p-6">
          <h4 className="font-semibold mb-4">Selected Object Details</h4>
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <div className="text-sm text-muted-foreground">Object Name</div>
                <div className="font-medium">{selectedNode.objectName}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Type</div>
                <Badge className="capitalize">{selectedNode.objectType}</Badge>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Dependency Level</div>
                <div className="font-medium">{selectedNode.dependencyLevel}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground">Deployment Order</div>
                <div className="font-medium">#{selectedNode.deploymentOrder}</div>
              </div>
            </div>
            
            <div>
              <div className="text-sm text-muted-foreground mb-2">Depends On</div>
              <div className="flex flex-wrap gap-2">
                {selectedNode.dependsOn.length === 0 ? (
                  <span className="text-sm text-muted-foreground">No dependencies</span>
                ) : (
                  selectedNode.dependsOn.map((dep, idx) => (
                    <Badge key={idx} variant="outline">
                      {dep.name} ({dep.type})
                    </Badge>
                  ))
                )}
              </div>
            </div>
            
            <div>
              <div className="text-sm text-muted-foreground mb-2">Referenced By</div>
              <div className="flex flex-wrap gap-2">
                {selectedNode.referencedBy.length === 0 ? (
                  <span className="text-sm text-muted-foreground">Not referenced</span>
                ) : (
                  selectedNode.referencedBy.map((ref, idx) => (
                    <Badge key={idx} variant="outline">
                      {ref.name} ({ref.type})
                    </Badge>
                  ))
                )}
              </div>
            </div>
          </div>
        </Card>
      )}

      <Card className="p-6">
        <h4 className="font-semibold mb-4">Deployment Order</h4>
        <div className="space-y-2">
          {mockDependencies
            .sort((a, b) => a.deploymentOrder - b.deploymentOrder)
            .map((node) => (
              <div key={node.objectId} className="flex items-center gap-3 p-3 bg-muted/30 rounded border">
                <Badge variant="secondary" className="w-12 justify-center">
                  #{node.deploymentOrder}
                </Badge>
                <div className="flex-1">
                  <div className="font-medium">{node.objectName}</div>
                  <div className="text-xs text-muted-foreground capitalize">{node.objectType}</div>
                </div>
                <Badge variant="outline">Level {node.dependencyLevel}</Badge>
              </div>
            ))}
        </div>
      </Card>
    </div>
  )
}
