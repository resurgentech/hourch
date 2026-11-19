package ui

import (
	"fmt"

	"github.com/resurgentech/hourch/daemon/internal/store"
)

// Graph is a server-rendered layered layout of a run's jobs: a job's column
// is the length of its longest dependency chain, rows keep submission order
// within a column, edges go from a dependency's right edge to the job's left
// edge. Rendered as SVG by run.html (no script).
type Graph struct {
	W, H  int
	Nodes []Node
	Edges []Edge
}

// Node is one job box.
type Node struct {
	ID, Name, Label, Status string
	X, Y, W, H              int
	TX, TY, SY              int // label and status text positions
}

// Edge is one dependency arrow as an SVG path.
type Edge struct {
	D      string
	Status string // status of the upstream job
}

const (
	boxW, boxH = 168, 44
	gapX, gapY = 56, 14
	pad        = 8
	labelMax   = 22
)

func layout(jobs []store.Job) Graph {
	byName := map[string]store.Job{}
	for _, j := range jobs {
		byName[j.Name] = j
	}
	depth := map[string]int{}
	var visit func(name string, seen map[string]bool) int
	visit = func(name string, seen map[string]bool) int {
		if d, ok := depth[name]; ok {
			return d
		}
		if seen[name] { // cannot happen (validated acyclic); be safe
			return 0
		}
		seen[name] = true
		d := 0
		for _, dep := range byName[name].DependsOn {
			if _, ok := byName[dep]; ok {
				d = max(d, visit(dep, seen)+1)
			}
		}
		depth[name] = d
		return d
	}
	rows := map[int]int{}
	pos := map[string]Node{}
	var g Graph
	for _, j := range jobs {
		col := visit(j.Name, map[string]bool{})
		row := rows[col]
		rows[col]++
		x, y := pad+col*(boxW+gapX), pad+row*(boxH+gapY)
		label := j.Name
		if r := []rune(label); len(r) > labelMax {
			label = string(r[:labelMax-1]) + "…"
		}
		n := Node{ID: j.ID, Name: j.Name, Label: label, Status: j.Status, X: x, Y: y, W: boxW, H: boxH,
			TX: x + 10, TY: y + 18, SY: y + 35}
		pos[j.Name] = n
		g.Nodes = append(g.Nodes, n)
		g.W = max(g.W, x+boxW+pad)
		g.H = max(g.H, y+boxH+pad)
	}
	for _, j := range jobs {
		to := pos[j.Name]
		for _, dep := range j.DependsOn {
			from, ok := pos[dep]
			if !ok {
				continue
			}
			x1, y1 := from.X+from.W, from.Y+from.H/2
			x2, y2 := to.X, to.Y+to.H/2
			mx := (x1 + x2) / 2
			g.Edges = append(g.Edges, Edge{D: fmt.Sprintf("M%d %d C%d %d %d %d %d %d", x1, y1, mx, y1, mx, y2, x2-4, y2), Status: from.Status})
		}
	}
	if g.W == 0 {
		g.W, g.H = 2*pad, 2*pad
	}
	return g
}
