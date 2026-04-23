package preflight

import (
	"errors"
	"fmt"
	"sort"
)

// diagnoseNetwork 基于同一 workload 的完整报告集合，诊断仅由 network 引起的失败。
// 它只会把双向失败归因到节点上，并把不对称失败单独保留下来。
//
// 实现方式：
//   - 先校验所有报告都属于同一个 workload 视图，并且报告集合是完整的。
//   - 对每一对节点做双向比较，只把双向失败建成无向图。
//   - 把这张图拆成多个连通分量，并对每个分量做精确求解。
//   - 把各分量的结果合并成全局的 bad 和 suspect 节点集合。
//   - 把方向性失败单独返回，避免污染节点归因结果。
func diagnoseNetwork(reports []*Report) (*networkDiagnosis, error) {
	if len(reports) == 0 {
		return nil, errors.New("empty reports")
	}

	// 只有当所有报告描述的是同一个 workload 视图时，后续求解才有意义。
	owners := make(map[string]*Report, len(reports))
	workload := reports[0].Workload
	worldSize := reports[0].WorldSize

	for _, report := range reports {
		if report == nil {
			return nil, errors.New("nil report")
		}
		if report.NodeName == "" {
			return nil, errors.New("report node name is empty")
		}
		if report.Workload != workload {
			return nil, fmt.Errorf("mixed workload: %q != %q", report.Workload, workload)
		}
		if report.WorldSize != worldSize {
			return nil, fmt.Errorf("mixed world size: %d != %d", report.WorldSize, worldSize)
		}
		if _, exists := owners[report.NodeName]; exists {
			return nil, fmt.Errorf("duplicated owner: %s", report.NodeName)
		}
		owners[report.NodeName] = report
	}

	if worldSize != len(reports) {
		return nil, fmt.Errorf("incomplete reports: got %d, want %d", len(reports), worldSize)
	}

	// 先统一排序，保证建图和最终输出都是稳定的。
	nodes := make([]string, 0, len(owners))
	for owner := range owners {
		nodes = append(nodes, owner)
	}
	sort.Strings(nodes)

	// adj 表示双向失败无向图：只有当 i->j 和 j->i 都失败时，adj[i][j] 才为 true。
	adj := make([][]bool, len(nodes))
	for i := range adj {
		adj[i] = make([]bool, len(nodes))
	}

	directional := make([]directionalAnomaly, 0)
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			// 这里只遍历 i < j，保证每一对无序节点只处理一次，同时仍然读取两个方向的原始结果。
			reportI := owners[nodes[i]]
			reportJ := owners[nodes[j]]

			ij, okIJ := reportI.Checks.Network.Target[nodes[j]]
			ji, okJI := reportJ.Checks.Network.Target[nodes[i]]
			if !okIJ || !okJI {
				return nil, fmt.Errorf("missing network target between %s and %s", nodes[i], nodes[j])
			}

			// 只有双向失败会进入节点归因；单向失败会被保存为方向性异常，并排除在故障图之外。
			if ij == CheckResultFail && ji == CheckResultFail {
				adj[i][j] = true
				adj[j][i] = true
				continue
			}

			if ij != ji {
				if ij == CheckResultFail {
					directional = append(directional, directionalAnomaly{From: nodes[i], To: nodes[j]})
				} else {
					directional = append(directional, directionalAnomaly{From: nodes[j], To: nodes[i]})
				}
			}
		}
	}

	// 目标函数在不同连通分量之间是可加的，所以每个分量都可以独立求解。
	components := buildComponents(nodes, adj)
	badNodes := make(map[string]struct{})
	suspectNodes := make(map[string]struct{})

	for _, component := range components {
		if len(component.edges) == 0 {
			continue
		}

		// required 节点出现在所有最小代价解释中；optional 节点至少出现在一个最优解释中，但不是所有解释都需要它。
		required, optional := solveComponentExactly(component)
		for _, node := range required {
			badNodes[node] = struct{}{}
			delete(suspectNodes, node)
		}
		for _, node := range optional {
			if _, exists := badNodes[node]; !exists {
				suspectNodes[node] = struct{}{}
			}
		}
	}

	// 对导出结果排序，避免 map 遍历顺序影响测试和调用方。
	sort.Slice(directional, func(i, j int) bool {
		if directional[i].From != directional[j].From {
			return directional[i].From < directional[j].From
		}
		return directional[i].To < directional[j].To
	})

	return &networkDiagnosis{
		badNodes:          sortedKeys(badNodes),
		suspectNodes:      sortedKeys(suspectNodes),
		directionalIssues: directional,
	}, nil
}

// buildComponents 把双向失败图拆成多个连通分量，这样每个分量都可以独立求解而不改变全局最优解。
//
// 实现方式：
//   - 对每个未访问节点，在双向失败邻接矩阵上跑一次 BFS。
//   - 把 BFS 到达的所有节点收集成一个连通分量。
//   - 把分量重新编码成局部节点切片和局部边列表，方便求解器处理。
//   - 返回所有分量，包括孤立节点，由调用方决定哪些分量需要求解。
func buildComponents(nodes []string, adj [][]bool) []graphComponent {
	visited := make([]bool, len(nodes))
	components := make([]graphComponent, 0)

	for start := range nodes {
		if visited[start] {
			continue
		}

		// 这次 BFS 恰好会找出以 start 为起点的一个完整连通分量。
		queue := []int{start}
		visited[start] = true
		members := make([]int, 0)

		for len(queue) > 0 {
			vertex := queue[0]
			queue = queue[1:]
			members = append(members, vertex)

			for peer := range nodes {
				if visited[peer] || !adj[vertex][peer] {
					continue
				}
				visited[peer] = true
				queue = append(queue, peer)
			}
		}

		// 重新建立局部索引，避免求解器处理全局节点位置。
		component := graphComponent{
			nodes: make([]string, 0, len(members)),
			edges: make([]graphEdge, 0),
		}
		for _, member := range members {
			component.nodes = append(component.nodes, nodes[member])
		}
		for i := 0; i < len(members); i++ {
			for j := i + 1; j < len(members); j++ {
				if adj[members[i]][members[j]] {
					component.edges = append(component.edges, graphEdge{U: i, V: j})
				}
			}
		}

		components = append(components, component)
	}

	return components
}

// solveComponentExactly 根据分量大小选择精确求解器，同时保证两条路径使用同一套最小代价语义。
//
// 实现方式：
//   - 小分量直接做全集枚举，因为实现简单且性能足够。
//   - 大分量切换到分支限界，避免扫描全部子集。
//   - 两种求解器优化的是同一个目标函数，所以无论选哪条路径，结果语义都一致。
func solveComponentExactly(component graphComponent) ([]string, []string) {
	if len(component.nodes) <= 16 {
		return solveByEnumeration(component)
	}

	return solveByBranchAndBound(component)
}

// solveByEnumeration 穷举所有节点子集。对小图来说，这是计算所有最优解交集和并集最直接的精确方法。
//
// 实现方式：
//   - 把每个 bitmask 视为一个候选“异常节点集合”。
//   - 每选中一个节点记 1 单位代价，每条仍未被覆盖的边也记 1 单位代价。
//   - 维护当前观察到的最小代价。
//   - 对所有最小代价解做交集得到 required 节点，做并集得到至少出现在一个最优解里的节点。
func solveByEnumeration(component graphComponent) ([]string, []string) {
	bestCost := -1
	var allMask uint64
	var anyMask uint64

	limit := uint64(1) << len(component.nodes)
	for mask := range limit {
		// mask 中置位的 bit 表示“这个节点被解释为异常节点”。
		selected := bitCount(mask)
		uncovered := 0
		// 每条未覆盖边都按链路故障计费；每个被选中的节点都按节点故障计费。
		// 这个目标函数的最小值决定最终诊断结果。
		for _, edge := range component.edges {
			if ((mask>>edge.U)&1) == 1 || ((mask>>edge.V)&1) == 1 {
				continue
			}
			uncovered++
		}
		cost := selected + uncovered

		if bestCost == -1 || cost < bestCost {
			// 首次遇到更优解时，直接替换当前维护的最优解集合。
			bestCost = cost
			allMask = mask
			anyMask = mask
			continue
		}
		if cost == bestCost {
			// 对等价最优解做交并运算，从而得到 bad 和 suspect 的语义。
			allMask &= mask
			anyMask |= mask
		}
	}

	return maskToNodes(component.nodes, allMask), maskToNodes(component.nodes, anyMask&^allMask)
}

// solveByBranchAndBound 与穷举保持同一套最优解语义，但不再显式扫描全部节点子集。
//
// 实现方式：
//   - 递归维护当前已经选中的异常节点集合。
//   - 找到在当前选择下第一条仍未被覆盖的边。
//   - 对这条边做分支：要么归因左端点，要么归因右端点。
//   - 如果当前已选节点数已经不可能优于最优解，就直接剪枝。
//   - 当不存在未覆盖边时，把这个最优解折叠进 bad/suspect 所需的交并集状态中。
func solveByBranchAndBound(component graphComponent) ([]string, []string) {
	bestCost := len(component.edges)
	allRequired := make(map[int]bool)
	anyOptional := make(map[int]bool)
	bestInitialized := false

	var dfs func(selected map[int]struct{}, edgePos int)
	dfs = func(selected map[int]struct{}, edgePos int) {
		// 当前选中的节点数如果已经超过最优代价上界，就不可能再得到更好的结果，
		// 因为未覆盖边只会继续增加非负代价。
		if len(selected) > bestCost {
			return
		}

		next := -1
		// 从当前位置向后扫描，找到第一条在当前选择下仍然需要解释的边。
		for index := edgePos; index < len(component.edges); index++ {
			edge := component.edges[index]
			_, hasU := selected[edge.U]
			_, hasV := selected[edge.V]
			if !hasU && !hasV {
				next = index
				break
			}
		}

		if next == -1 {
			cost := len(selected)
			// 走到这里说明图中每条边都已经被至少一个选中端点覆盖，
			// 剩余代价就等于当前选中的节点数。
			if !bestInitialized || cost < bestCost {
				bestCost = cost
				bestInitialized = true
				allRequired = make(map[int]bool, len(selected))
				anyOptional = make(map[int]bool, len(selected))
				for node := range selected {
					allRequired[node] = true
					anyOptional[node] = true
				}
				return
			}
			if cost == bestCost {
				// 等价最优解会收紧 required 集合，同时扩大 optional 集合。
				for node := range allRequired {
					if _, ok := selected[node]; !ok {
						delete(allRequired, node)
					}
				}
				for node := range selected {
					anyOptional[node] = true
				}
			}
			return
		}

		edge := component.edges[next]

		// 对第一条未覆盖边，只存在两种节点归因方式：归因左端点，或归因右端点。
		left := cloneSet(selected)
		left[edge.U] = struct{}{}
		dfs(left, next+1)

		right := cloneSet(selected)
		right[edge.V] = struct{}{}
		dfs(right, next+1)
	}

	dfs(make(map[int]struct{}), 0)

	required := make([]string, 0, len(allRequired))
	optional := make([]string, 0)
	for node := range allRequired {
		required = append(required, component.nodes[node])
	}
	for node := range anyOptional {
		if !allRequired[node] {
			optional = append(optional, component.nodes[node])
		}
	}
	sort.Strings(required)
	sort.Strings(optional)

	return required, optional
}

// cloneSet 复制当前 DFS 状态，保证不同递归分支互不影响，同时保持状态表达清晰。
//
// 实现方式：
//   - 先分配一个和源 map 容量接近的新 map。
//   - 把所有已选节点拷贝进去。
//   - 返回这份拷贝，让一个递归分支的修改不会影响另一个分支。
func cloneSet(src map[int]struct{}) map[int]struct{} {
	dst := make(map[int]struct{}, len(src))
	for key := range src {
		dst[key] = struct{}{}
	}

	return dst
}

// bitCount 供小图精确求解器快速计算“选中了多少节点”。
//
// 实现方式：
//   - 通过 `mask &= mask - 1` 反复清除最低位的 1。
//   - 统计这个过程需要执行多少次，直到 mask 变成 0。
//   - 这个次数就是候选 bitmask 中被选中的节点数量。
func bitCount(mask uint64) int {
	count := 0
	for mask != 0 {
		mask &= mask - 1
		count++
	}

	return count
}

// maskToNodes 把 bitmask 形式的解还原成稳定顺序的节点名切片。
//
// 实现方式：
//   - 按局部节点切片的索引顺序遍历。
//   - 检查对应 bit 是否在 mask 里被置位。
//   - 把命中的节点名按稳定顺序追加到输出切片中。
func maskToNodes(nodes []string, mask uint64) []string {
	result := make([]string, 0)
	for i := range nodes {
		if ((mask >> i) & 1) == 1 {
			result = append(result, nodes[i])
		}
	}

	return result
}

// sortedKeys 把集合型 map 转成有序切片，保证导出结果对测试和调用方都是稳定的。
//
// 实现方式：
//   - 先把 map 中的每个元素拷贝到切片里。
//   - 再按字典序排序。
//   - 返回排序后的切片，避免导出结果依赖 Go 的 map 遍历顺序。
func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)

	return result
}
