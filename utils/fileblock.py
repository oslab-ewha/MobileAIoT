import heapq
import copy
#-------------------------------------------------------
class FileBlock:
    def __init__(self, blknum, last_ref_vtime=0, write_cnt=0, inode=-1, priority_value=float('inf')):
        self.addr = blknum
        self.inode = inode
        self.modified_bit = 0    # dirty bit
        self.write_cnt = write_cnt
        self.last_ref_vtime = last_ref_vtime

    def set_modified(self, bit):
        self.modified_bit = bit

    def set_reference(self, vtime):
        self.last_ref_vtime = vtime

    def __hash__(self):
        '''
        >>> d = { FileBlock(0): 0, FileBlock(2): 2, FileBlock(7): 7 }
        >>> type(d)
        <class 'dict'>
        >>> d
        {<__main__.FileBlock object at 0x747a258635d0>: 0, <__main__.FileBlock object at 0x747a25863810>: 2, <__main__.FileBlock object at 0x747a25874050>: 7}
        '''
        return hash(self.addr)

    def __eq__(self, other):
        '''
        Dunder(double underbar) method in Python classes which defines the functionality of the equality operator (==)
        * The `FileBlock` class checks the equivalence of elements in either `int` or `FileBlock` classes
        >>> a = FileBlock(7)
        >>> b = FileBlock(6)
        >>> c = FileBlock(7)
        >>> a == b
        False
        >>> a == c
        True
        >>> a == 7
        True
        '''
        if (isinstance(other, self.__class__)) and (self.addr == other.addr):
            return True
        elif self.addr == other:
            return True
        else:
            return False

    def __ne__(self, other):
        '''
        Dunder method in Python classes which defines the functionality of the inequality operator (!=)
        * The `FileBlock` class checks the inequivalence of elements in either `int` or `FileBlock` classes
        '''
        if (isinstance(other, self.__class__)) and (self.addr != other.addr):
            return True
        elif self.addr != other:
            return True
        else:
            return False
#-------------------------------------------------------
class NVM_FileBlock:
    def __init__(self, blknum, last_ref_vtime=0, reference_cnt=0, inode=-1, history_bit=1):
        self.addr = blknum
        self.last_ref_vtime = last_ref_vtime    # updated_time
        self.modified_bit = 0    # dirty bit
        self.reference_cnt = reference_cnt
        self.inode = inode
        self.history_bit = history_bit
        self.shadow_reference_cnt = 0
        self.decay_history_bit = 0 # for decay

    def set_modified(self, bit=1):
        self.modified_bit = bit

    def set_reference(self, vtime, set_reference=False, window_size=8, decay=None):
        time_interval = vtime - self.last_ref_vtime
        if time_interval <= window_size:
            out_length = (self.history_bit.bit_length() + time_interval) - window_size
            if out_length > 0:
                updated_history_bit = self.history_bit & ((1 << (self.history_bit.bit_length() - out_length)) - 1)
            else:
                updated_history_bit = (self.history_bit << time_interval)
            updated_history_bit += int(set_reference)
            self.history_bit = updated_history_bit
        else:
            self.history_bit = 0
        if self.history_bit.bit_count() == 0:
            self.shadow_reference_cnt += self.reference_cnt

        # decay
        if (decay is not None):
            # `(1 << (i + 1)) - 1` : Generate a bitmask where the lowest (i+1) bits are all set to 1
            freq_window = [(self.history_bit & ((1 << (i + 1)) - 1)).bit_count() for i in range(self.history_bit.bit_length())]
            freq_window = [0]*(len(decay) - len(freq_window)) + freq_window
            decay_count = sum([1 for i,j in enumerate(decay) if (isinstance(j, self.__class__) and freq_window[i] > j.reference_cnt) ])

        self.last_ref_vtime = vtime
        self.reference_cnt = self.history_bit.bit_count()
        self.reference_cnt -= (decay_count*0.5)

    def is_same_loop(self, other):
        if not isinstance(other, self.__class__):
            return False
        elif self.reference_cnt == other.reference_cnt and self.history_bit == other.history_bit:
            return True
        else:
            return False

    def __hash__(self):
        '''
        >>> d = { FileBlock(0): 0, FileBlock(2): 2, FileBlock(7): 7 }
        >>> type(d)
        <class 'dict'>
        >>> d
        {<__main__.FileBlock object at 0x747a258635d0>: 0, <__main__.FileBlock object at 0x747a25863810>: 2, <__main__.FileBlock object at 0x747a25874050>: 7}
        '''
        return hash(self.addr)

    def __eq__(self, other):
        '''
        Dunder(double underbar) method in Python classes which defines the functionality of the equality operator (==)
        * The `FileBlock` class checks the equivalence of elements in either `int` or `FileBlock` classes
        >>> a = FileBlock(7)
        >>> b = FileBlock(6)
        >>> c = FileBlock(7)
        >>> a == b
        False
        >>> a == c
        True
        >>> a == 7
        True
        '''
        try:
            return (self.addr == other.addr)
        except:
            return (self.addr == other)

    def __lt__(self, other):
        '''
        Defines behavior for the less-than operator (<)
        '''
        try:
            if (self.reference_cnt < other.reference_cnt):
            # if (self.history_bit.bit_count() < other.history_bit.bit_count()):
                return True
            elif (self.reference_cnt == other.reference_cnt): # Tie!!!
                if (self.reference_cnt == 0):
                    if (self.shadow_reference_cnt < other.shadow_reference_cnt):
                        return True
                    elif ((self.shadow_reference_cnt == other.shadow_reference_cnt)
                        and (self.addr > other.addr)):
                        return True
                # 2nd criterion
                s_h = self.history_bit
                o_h = other.history_bit
                if ((s_h & -s_h).bit_length() < (o_h & -o_h).bit_length()):
                    return True
                elif ((s_h & -s_h).bit_length() == (o_h & -o_h).bit_length()
                      and self.addr > other.addr): # 3rd criterion
                    return True
            return False

        except:
            return self.reference_cnt < other

    def __gt__(self, other):
        '''
        Defines behavior for the greater-than operator (>)
        '''
        try:
            if (self.reference_cnt > other.reference_cnt):
                return True
            elif (self.reference_cnt == other.reference_cnt): # Tie!!!
                if (self.reference_cnt == 0):
                    if (self.shadow_reference_cnt > other.shadow_reference_cnt):
                        return True
                    elif ((self.shadow_reference_cnt == other.shadow_reference_cnt)
                        and (self.addr < other.addr)):
                        return True
                # 2nd criterion
                s_h = self.history_bit
                o_h = other.history_bit
                if ((s_h & -s_h).bit_length() > (o_h & -o_h).bit_length()):
                    return True
                elif ((s_h & -s_h).bit_length() == (o_h & -o_h).bit_length()
                      and self.addr < other.addr): # 3rd criterion
                    return True
            return False

        except:
            return self.reference_cnt > other
