package async_http_server_router_v2

import "base:builtin"
import "core:hash/xxhash"
import "core:strings"
import "core:testing"

@(private)
INVALID :: -1

@(private)
MAX_DEPTH :: 16

@(private = "file")
Node_Type :: enum {
	Static,
	Field,
}

@(private = "file")
Node :: struct {
	type:  Node_Type,
	id:    int,
	hash:  u64,
	child: int,
	next:  int,
}

@(private)
Index_Iterator :: struct {
	segments:    [MAX_DEPTH]u64,
	seg_count:   int,
	stack_node:  [MAX_DEPTH]int,
	stack_depth: [MAX_DEPTH]int,
	stack_top:   int,
	initialized: bool,
}

@(private)
Index :: struct {
	nodes: [dynamic]Node,
	root:  int,
}

@(private)
index_init :: proc(self: ^Index) {
	self.nodes = make([dynamic]Node)
	self.root = INVALID
}

@(private)
index_deinit :: proc(self: ^Index) {
	builtin.delete(self.nodes)
	self.root = INVALID
}

@(private)
index_add :: proc(self: ^Index, id: int, route: string) {
	if self.root == INVALID do self.root = create_node(self, .Static, hash(""))

	if route == "/" || route == "" {
		self.nodes[self.root].id = id
		return
	}

	current := self.root
	rest := route

	for seg in strings.split_iterator(&rest, "/") {
		if len(seg) == 0 do continue

		is_param := len(seg) >= 2 && seg[0] == '{' && seg[len(seg) - 1] == '}'
		type: Node_Type = is_param ? .Field : .Static
		seg_hash := is_param ? 0 : hash(seg)

		child_idx := self.nodes[current].child
		found := INVALID
		last_static := INVALID
		last_sibling := INVALID

		curr := child_idx
		for curr != INVALID {
			n := &self.nodes[curr]

			if n.type == type && (type == .Field || n.hash == seg_hash) {
				found = curr
				break
			}

			if n.type == .Static do last_static = curr
			last_sibling = curr
			curr = n.next
		}

		if found != INVALID {
			current = found
		} else {
			new_node_idx := create_node(self, type, seg_hash)

			if child_idx == INVALID do self.nodes[current].child = new_node_idx
			else if type == .Static {
				if last_static == INVALID {
					self.nodes[new_node_idx].next = self.nodes[current].child
					self.nodes[current].child = new_node_idx
				} else {
					self.nodes[new_node_idx].next = self.nodes[last_static].next
					self.nodes[last_static].next = new_node_idx
				}
			} else do self.nodes[last_sibling].next = new_node_idx
			current = new_node_idx
		}
	}

	self.nodes[current].id = id
}

@(private)
index_iter_init :: proc(it: ^Index_Iterator, route: string) {
	it.seg_count = 0
	it.stack_top = 0
	it.initialized = true

	if route == "/" || route == "" do return

	rest := route
	for seg in strings.split_iterator(&rest, "/") {
		if len(seg) == 0 do continue
		if it.seg_count < MAX_DEPTH {
			it.segments[it.seg_count] = hash(seg)
			it.seg_count += 1
		}
	}
}

@(private)
index_iter :: proc(self: ^Index, it: ^Index_Iterator) -> (int, bool) {
	if !it.initialized || self.root == INVALID do return INVALID, false

	if it.seg_count == 0 {
		if it.stack_top == 0 {
			it.stack_top = 1
			if self.nodes[self.root].id != INVALID do return self.nodes[self.root].id, true
		}
		return INVALID, false
	}

	if it.stack_top == 0 do push_children(self, it, self.nodes[self.root].child, 0)

	for it.stack_top > 0 {
		it.stack_top -= 1
		node_idx := it.stack_node[it.stack_top]
		depth := it.stack_depth[it.stack_top]

		node := &self.nodes[node_idx]
		target_hash := it.segments[depth]

		match := (node.type == .Static && node.hash == target_hash) || (node.type == .Field)

		if match {
			if depth == it.seg_count - 1 {
				if node.id != INVALID do return node.id, true
			} else {
				push_children(self, it, node.child, depth + 1)
			}
		}
	}

	return INVALID, false
}

@(private = "file")
push_children :: proc(self: ^Index, it: ^Index_Iterator, first_child: int, depth: int) {
	curr := first_child
	if curr == INVALID do return

	start_top := it.stack_top
	for curr != INVALID {
		if it.stack_top < MAX_DEPTH {
			it.stack_node[it.stack_top] = curr
			it.stack_depth[it.stack_top] = depth
			it.stack_top += 1
		}
		curr = self.nodes[curr].next
	}

	end_top := it.stack_top - 1
	for i := start_top; i < end_top; i += 1 {
		it.stack_node[i], it.stack_node[end_top] = it.stack_node[end_top], it.stack_node[i]
		end_top -= 1
	}
}

@(private = "file")
create_node :: proc(self: ^Index, type: Node_Type, hash: u64 = 0) -> int {
	node := Node {
		type  = type,
		id    = INVALID,
		hash  = hash,
		child = INVALID,
		next  = INVALID,
	}
	append(&self.nodes, node)
	return len(self.nodes) - 1
}

@(private = "file")
hash :: #force_inline proc(segment: string) -> u64 {
	return xxhash.XXH64(transmute([]u8)(segment))
}

@(test)
test_router_find :: proc(t: ^testing.T) {
	index: Index
	index_init(&index)
	defer index_deinit(&index)

	index_add(&index, 0, "/user/{id}")
	index_add(&index, 1, "/user/name")
	index_add(&index, 2, "/board")

	it: Index_Iterator
	index_iter_init(&it, "/user/name")

	found_id: int
	for id in index_iter(&index, &it) {
		found_id = id
		break
	}

	testing.expect_value(t, found_id, 1)
}

