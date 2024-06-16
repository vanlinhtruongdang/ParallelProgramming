import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Array
from ctypes import c_int64

import numpy as np

def dictMerge(dictA, dictB):
    return {**dictA, **dictB}

def preSum(A, L, R, preSumDict = {}):    
    if not (0 <= L <= R <= len(A)):
        return None
    
    if (L == R):
        preSumDict[L, R] = A[L]

        return A[L], preSumDict
    
    mid = (L + R) // 2
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        leftSum, leftDict = executor.submit(preSum, A, L, mid).result()
        rightSum, rightDict = executor.submit(preSum, A, mid + 1, R).result()

        sumLR = leftSum + rightSum
        mergeDict = dictMerge(leftDict, rightDict)
        mergeDict[L, R] = sumLR
        
        return sumLR, mergeDict


def prefixSum(A, L, R, offset, preSumDict):
    global prefixSumArray

    if not (0 <= L <= R <= len(A)):
        return
    
    if (L == R):
        prefixSumArray[L] = A[L] + offset
        return
    
    mid = (L + R) // 2
    leftSum = preSumDict[L, mid]

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        executor.submit(prefixSum, A, L, mid, offset, preSumDict).result()
        executor.submit(prefixSum, A, mid + 1, R, offset + leftSum, preSumDict).result()

    
if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(0, 10, size=10)
    print(f'Input array: {A}')

    _, preSumDict = preSum(A, 0, len(A) - 1)
    prefixSumArray = Array(c_int64, [0] * len(A), lock=False)

    prefixSum(A, 0, len(A) - 1, 0, preSumDict)

    print(f'Prefix-sum array: {prefixSumArray[:]}')
