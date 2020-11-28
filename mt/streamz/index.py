'''Utitilites related to indexing.'''


from mt.base.deprecated import deprecated_func
import mt.base.functional as _bf
import mt.base.hashing as _bh


__all__ = ['IndexGrouping']


class IndexGrouping(object):
    '''Parititons a collection of items into groups and returns the list of indices for each group.

    Parameters
    ----------
    item_cnt : int or None
        number of items in the collection. If None is provided, item_cnt is assumed infinity.
    max_group_size : int
        maximum number of items per group
    policy : {'sequential', 'repeated'}
        policy to partition the collection. Value 'sequential' means the collection is grouped sequentially into `[0, max_group_size), [max_group_size, max_group_size*2), ..., [X, item_cnt)`. If item_cnt is None, then we assume item_cnt is infinity. Value 'repeated' means the items can be repeated beyond `item_cnt` and rolled back to [0, item_cnt) range by taking moduli.
    '''
    def __init__(self, item_cnt, max_group_size, policy='sequential'):

        if (item_cnt is not None) and (not isinstance(item_cnt, int) or item_cnt <= 0):
            raise ValueError("Argument 'item_cnt' must be None or a positive integer, but {} was provided.".format(item_cnt))
        self.item_cnt = item_cnt
        
        if not isinstance(max_group_size, int) or max_group_size <= 0:
            raise ValueError("Argument 'max_group_size' must be a positive integer, but {} was provided.".format(max_group_size))
        self.max_group_size = max_group_size

        if policy not in ['sequential', 'repeated']:
            raise ValueError("Policy must be either 'sequential' or 'repeated', but {} was provided.".format(policy))
        self.policy = policy

    def group_count(self):
        '''Returns the number of groups, or None if either 'repeated' policy was provided or ('sequential' policy was provided and 'item_cnt' was None).'''
        if self.policy == 'sequential':
            return None if self.item_cnt is None else (self.item_cnt + self.max_group_size - 1) // self.max_group_size
        if self.policy == 'repeated':
            return None

    def get_indices(self, group_id):
        '''Returns the list of indices for a given group.
        
        Parameters
        ----------
        group_id : int
            zero-based group index

        Returns
        -------
        list
            list of indices of the group
        '''
        if not isinstance(group_id, int) or group_id < 0:
            raise ValueError("Argument 'group_id' must be a non-negative integer, but {} was provided.".format(group_id))

        if self.item_cnt is None:
            start_id = group_id*self.max_group_size
            end_id = (group_id+1)*self.max_group_size
            return [x for x in range(start_id, end_id)]

        if self.policy == 'sequential':
            start_id = group_id*self.max_group_size
            if start_id >= self.item_cnt:
                raise ValueError("Group {} for a collection of {} items with group size {} and sequential policy does not exist.".format(group_id, self.item_cnt, self.max_group_size))
            end_id = min(self.item_cnt, (group_id+1)*self.max_group_size)
            return [x for x in range(start_id, end_id)]

        if policy=='repeated':
            start_id = group_id*self.max_group_size
            end_id = (group_id+1)*self.max_group_size
            return [x%self.item_cnt for x in range(start_id, end_id)]

    def __call__(self, group_id):
        return self.get_indices(group_id)
    __call__.__doc__ = get_indices.__doc__


@deprecated_func("0.0.8", suggested_func="mt.base.hashing.hash_int", removed_version="0.2.0", docstring_prefix="    ")
def hash_int(i):
    '''Knuth's cheap integer hash.

    `Source <https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key>`_

    Parameters
    ----------
    i : int
        an integer to hash
    
    Returns
    -------
    int
        an unsigned 32-bit hashed integer
    '''
    return _bh.hash_int(i)


@deprecated_func("0.0.8", suggested_func="mt.base.functional.on_list", removed_version="0.2.0", docstring_prefix="    ")
def on_list(func):
    '''Turns a function that operates on each element at a time into a function that operates on a colletion of elements at a time. Can be used as a decorator.

    Parameters
    ----------
    func : function
        the function to build upon. Its expected form is `def func(x, *args, **kwargs) -> object`.

    Returns
    -------
    function
        a wrapper function `def func_on_list(list_x, *args, **kwargs) -> list_of_objects` that invokes `func(x, *args, **kwargs)` for each x in list_x.
    '''
    return _bf.on_list(func)


@deprecated_func("0.0.8", suggested_func="mt.base.functional.join_funcs", removed_version="0.2.0", docstring_prefix="    ")
def join_funcs(*funcs, left_to_right=True):
    '''Concatenate functions taking a single argument into a single function.

    Parameters
    ----------
    *funcs : list
        list of functions f1(x), f2(x), ..., fn(x)
    left_to_right : boolean
        whether to concatenate from left to right or from right to left.

    Returns
    -------
    func : function
        The function `f(x) = fn(...f2(f1(x))...)` if left_to_right is True, else `f(x) = f1(f2(...fn(x)...))`.
    '''
    return _bf.join_funcs(*funcs, left_to_right=left_to_right)
