import heapq
import copy
import random
from .fileblock import FileBlock, NVM_FileBlock

class FileCache():
    def __init__(self, max_cache_size, ratio, write_buffer_max=None):
        self.max_cache_size = max_cache_size
        self.buffer_cache = BufferCache(max_cache_size=max_cache_size)
        if write_buffer_max is None:
            self.write_buffer = WriteBuffer(max_cache_size=int(round(ratio*max_cache_size, 0)))
        else:
            self.write_buffer = WriteBuffer(max_cache_size=int(round(ratio*write_buffer_max, 0)))

        self.hit_cnt = 0
        self.miss_cnt = 0
        self.stor_flush_cnt = 0
        self.w_buffer_write_cnt = 0

    def reference(self, cur_vtime, cur_rtime, operation, blknum, inode):
        if blknum in self.buffer_cache.cache: # cache hit
            self.hit_cnt += 1
            self.buffer_cache.reference(cur_vtime, operation=operation, blknum=blknum, inode=inode)
            if operation == 'write':
                self.buffer_cache.cache[blknum].set_modified(1)
                assert self.buffer_cache.cache[blknum].modified_bit == 1

        else: # cache miss
            self.miss_cnt += 1
            write_cnt = 0

            if (blknum in self.write_buffer.cache): # cache miss. block is in write buffer
                write_cnt = self.write_buffer.cache[blknum].write_cnt
                if operation == 'write':
                    self.w_buffer_write_cnt += 1

            if self.buffer_cache.is_full():
                victim_block = self.buffer_cache.evict()
                if victim_block.modified_bit:
                    self.sync_to_NVM(victim_block, cur_vtime, cur_rtime)

            self.buffer_cache.reference(cur_vtime, operation=operation, blknum=blknum, inode=inode, write_cnt=write_cnt)

    def sync_to_NVM(self, flushed, cur_vtime, cur_rtime):
        not_in = [f for f in flushed if f not in set(self.write_buffer.cache.keys())]
        in_cache = set(flushed) - set(not_in)

        for file_block in in_cache:
            self.write_buffer.reference(cur_rtime, operation='flush', blknum=file_block.addr, inode=file_block.inode,
                                        write_cnt=file_block.write_cnt)
            self.w_buffer_write_cnt += 1
            file_block.set_modified(0)

        evicted_num = len(self.write_buffer) + len(not_in) - self.write_buffer.max_cache_size
        evicted_num = len(self.write_buffer) if evicted_num > len(self.write_buffer) else evicted_num
        for i in range(evicted_num):
            victim_block = self.write_buffer.evict()
            self.stor_flush_cnt += 1 # flush
            # Flush data from the write buffer, so do not change the modified bit in the buffer cache
            # self.buffer_cache.cache[victim_block.addr].set_modified(0)
        for file_block in not_in:
            victim_block = None
            if self.write_buffer.is_full():
                victim_block = self.write_buffer.evict()

            self.write_buffer.reference(cur_rtime, operation='flush', blknum=file_block.addr, inode=file_block.inode,
                                        write_cnt=file_block.write_cnt)
            self.w_buffer_write_cnt += 1
            file_block.set_modified(0)

            if (victim_block is not None):
                self.stor_flush_cnt += 1 # flush

    def flush(self, cur_vtime, cur_rtime):
        self.write_buffer.vtime += 1
        self.write_buffer.shadow_hit_freq.append(float("inf"))
        self.write_buffer.while_cnt = 0

        flushed = []
        for blknum in random.sample(self.buffer_cache.replacement_priority, len(self.buffer_cache.replacement_priority)):
            file_block = self.buffer_cache.cache[blknum]

            if file_block.modified_bit:
                file_block.set_modified(0)
                flushed.append(file_block)

        self.sync_to_NVM(flushed, cur_vtime, cur_rtime)

        if len(self.write_buffer.shadow_hit_freq) > self.write_buffer.window_size:
            del self.write_buffer.shadow_hit_freq[0]

        if len(flushed):
            return flushed
        else:
            return -1

#--------------------------------------------------------------------------------
class BufferCache:
    def __init__(self, max_cache_size):
        self.cache = {}   # {blknum <class 'int'> : file_block <class 'FileBlock'>}
        self.replacement_priority = []
        self.max_cache_size = max_cache_size
    
    def __len__(self):
        return len(self.cache)

    def is_full(self):
        return len(self.cache) >= self.max_cache_size

    def reference(self, cur_vtime, operation, blknum, inode, write_cnt=0):
        if blknum in self.cache:
            file_block = self.cache[blknum]
            idx = self.replacement_priority.index(file_block)
            file_block.set_reference(cur_vtime)

            if idx == 0:
                return
            else:
                _ = self.replacement_priority.pop(idx)
                self.replacement_priority.insert(0, blknum)
                return

        else:
            file_block = FileBlock(blknum, last_ref_vtime=cur_vtime, write_cnt=write_cnt, inode=inode)
            self.replacement_priority.insert(0, file_block)
            self.cache[blknum] = file_block
            return -1

    def evict(self):
        if len(self.cache) == 0:
            return None

        victim_file_block = self.replacement_priority.pop()
        self.cache.pop(victim_file_block.addr)

        return victim_file_block

    def aging(self):
        pass

#--------------------------------------------------------------------------------
class WriteBuffer():
    def __init__(self, max_cache_size, window_size=5):
        self.cache = {}  # {addr: file_block}
        self.vtime = 0 # count flush times
        self.window_size = window_size
        self.max_cache_size = max_cache_size
        self.main_heap = []
        self.second_list = []
        self.shadow_cache = {}    # {addr: file_block}
        self.shadow_hit_freq = []
        self.while_cnt = 0
        self.evict_cnt=0

    def __len__(self):
        return len(self.cache)

    def is_full(self):
        return len(self.cache) >= self.max_cache_size

    def heap_sort(self, idx, time):
        self.heap_siftup(idx, time)
        self.heap_siftdown(idx, time)

    def heap_siftdown(self, idx, time): # when the updated value is greater than before
        # if updated value is larger than left child or right child
        while True:
            left = idx * 2 + 1
            right = idx * 2 + 2
            s_idx = idx    # smallest_idx

            # update history bit
            for c_idx in [left, right]:    # child_idx
                if (c_idx < len(self.main_heap)):
                    if self.main_heap[c_idx].last_ref_vtime != time:
                        self.main_heap[c_idx].set_reference(time, window_size=self.window_size, decay=self.shadow_hit_freq)
                        self.heap_siftdown(idx=c_idx, time=time)

            # compare values
            if (left < len(self.main_heap)):
                if (self.main_heap[s_idx] > self.main_heap[left]):
                    s_idx = left

            if (right < len(self.main_heap)):
                if (self.main_heap[s_idx] > self.main_heap[right]):
                    s_idx = right

            if s_idx == idx:
                break

            self.main_heap[idx], self.main_heap[s_idx] = self.main_heap[s_idx], self.main_heap[idx]
            idx = s_idx

    def heap_siftup(self, idx, time): # when the updated value is smaller than before
        while idx > 0:
            p_idx = (idx - 1) // 2    # parent_idx
            # update history bit
            if self.main_heap[p_idx].last_ref_vtime != time:
                self.main_heap[p_idx].set_reference(time, window_size=self.window_size, decay=self.shadow_hit_freq)
                self.heap_siftup(idx=p_idx, time=time)

            if not (self.main_heap[p_idx] > self.main_heap[idx]):
                break

            self.main_heap[idx], self.main_heap[p_idx] = self.main_heap[p_idx], self.main_heap[idx]
            idx = p_idx

    def evict(self):
        self.heap_sort(idx=0, time=self.vtime)

        evict_candidate = []
        current_second = []
        victim = None

        current_while_cnt = 0

        while self.main_heap:
            self.while_cnt += 1; current_while_cnt += 1

            evicted = self.main_heap[0]

            if evicted.is_same_loop(self.shadow_hit_freq[-1]) or (evicted.last_ref_vtime == self.vtime and evicted.history_bit % 2 == 1):
                if (len(current_second) and current_second[-1].reference_cnt < evicted.reference_cnt) or (current_while_cnt >= 5 or self.while_cnt >= 50):
                    victim = heapq.heappop(self.main_heap) #evicted
                    break

                evict_candidate.append(evicted)
                _ = heapq.heappop(self.main_heap)
                continue

            if evicted.history_bit % 4 == 3 and evicted.last_ref_vtime == self.vtime: # 'Consecutive flush' rule
                current_second.append(evicted)
                _ = heapq.heappop(self.main_heap)
                continue

            if len(self.second_list) and self.second_list[0] < evicted:
                victim = heapq.heappop(self.second_list)
            elif len(current_second) and current_second[0].reference_cnt < evicted.reference_cnt:
                victim = heapq.heappop(current_second)
            else:
                victim = heapq.heappop(self.main_heap) #evicted
            break

        if victim is None:
            if len(self.second_list):
                victim = heapq.heappop(self.second_list)
            elif len(current_second):
                victim = heapq.heappop(current_second)
            else:
                for i, e in enumerate(evict_candidate):
                    victim = e
                    del evict_candidate[i]
                    break

        if victim.history_bit.bit_count() > 0:
            self.shadow_cache[victim.addr] = victim

        if len(evict_candidate) or len(self.second_list):
            self.main_heap.extend(evict_candidate + self.second_list)
            heapq.heapify(self.main_heap)
        self.second_list = current_second

        _ = self.cache.pop(victim.addr)

        return victim

    def reference(self, time, blknum, inode, operation='flush', write_cnt=-1):

        if blknum in self.cache.keys():
            file_block = self.cache[blknum]
            try:
                idx = self.main_heap.index(file_block)
                file_block.set_reference(self.vtime, set_reference=True, window_size=self.window_size, decay=self.shadow_hit_freq)
            except:
                self.second_list.remove(file_block)
                file_block.set_reference(self.vtime, set_reference=True, window_size=self.window_size, decay=self.shadow_hit_freq)
                heapq.heappush(self.main_heap, file_block)
                idx = self.main_heap.index(file_block)

            self.heap_sort(idx=idx, time=self.vtime)

            return

        elif blknum in self.shadow_cache.keys():
            victim = None
            updates = self.shadow_cache.pop(blknum)
            updates.set_reference(self.vtime, set_reference=True, window_size=self.window_size, decay=self.shadow_hit_freq)
            # for decay
            if (self.shadow_hit_freq[-1] == float("inf")) and (updates.reference_cnt >= 1):
                self.shadow_hit_freq[-1] = copy.deepcopy(updates)
            if len(self.cache) >= self.max_cache_size:
                victim = self.evict()
            # push new file_block
            self.cache[blknum] = updates
            heapq.heappush(self.main_heap, updates)

            return

        else:
            victim = None
            updates = NVM_FileBlock(blknum=blknum, last_ref_vtime=self.vtime, reference_cnt=1, inode=inode, history_bit=1)
            if len(self.cache) >= self.max_cache_size:
                victim = self.evict()
            # push new file_block
            self.cache[blknum] = updates
            heapq.heappush(self.main_heap, updates)

            return