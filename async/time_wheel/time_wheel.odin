package time_wheel

// https://docs.rs/timing-wheel/0.1.4/timing_wheel/

import "core:container/priority_queue"
import "core:time"

Task :: u64

Slot :: u64

Time_Wheel :: struct {
	start_time:    time.Tick,
	tick_interval: u64,
	current_ticks: u64,
	pq:            priority_queue.Priority_Queue(Slot),
	timers:        map[Slot][dynamic]Task,
	counter:       int,
}

init :: proc(self: ^Time_Wheel, tick_interval: time.Duration) {
	self.start_time = time.tick_now()
	self.tick_interval = u64(time.duration_microseconds(tick_interval))
	self.current_ticks = 0
	self.counter = 0
	self.timers = make(map[Slot][dynamic]Task)

	less_proc := proc(a, b: Slot) -> bool {return a < b}
	priority_queue.init(&self.pq, less_proc, priority_queue.default_swap_proc(Slot))
}

deinit :: proc(self: ^Time_Wheel) {
	priority_queue.destroy(&self.pq)
	for _, tasks in self.timers {
		delete(tasks)
	}
	delete(self.timers)
}

after :: proc(self: ^Time_Wheel, duration: time.Duration, task: Task) -> (u64, bool) {
	elapsed := u64(time.duration_microseconds(time.tick_since(self.start_time)))
	delay_micros := u64(time.duration_microseconds(duration))
	target_micros := elapsed + delay_micros

	target_ticks := target_micros / self.tick_interval
	if target_micros % self.tick_interval != 0 {
		target_ticks += 1
	}

	if target_ticks <= self.current_ticks {
		return 0, false
	}

	if target_ticks in self.timers {
		append(&self.timers[target_ticks], task)
	} else {
		tasks := make([dynamic]Task)
		append(&tasks, task)
		self.timers[target_ticks] = tasks
		priority_queue.push(&self.pq, target_ticks)
	}

	self.counter += 1
	return target_ticks, true
}

spin :: proc(self: ^Time_Wheel, expired_tasks: ^[dynamic]Task) {
	elapsed := u64(time.duration_microseconds(time.tick_since(self.start_time)))
	self.current_ticks = elapsed / self.tick_interval

	for priority_queue.len(self.pq) > 0 {
		next_tick := priority_queue.peek(self.pq)

		if next_tick > self.current_ticks {
			break
		}

		priority_queue.pop(&self.pq)

		if tasks, ok := self.timers[next_tick]; ok {
			self.counter -= len(tasks)
			for task in tasks {
				append(expired_tasks, task)
			}
			delete(tasks)
			delete_key(&self.timers, next_tick)
		}
	}
}

