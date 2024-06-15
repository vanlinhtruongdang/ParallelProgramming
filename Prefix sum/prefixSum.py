import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Array, Manager
from ctypes import c_int64

import numpy as np

def preSum(A, L, R, preSumDict):
    if not (0 <= L <= R <= len(A)):
        return None
    
    if (L == R):
        preSumDict[(L, L)] = A[L]
        return A[L]
    
    mid = (L + R) // 2
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        leftSum = executor.submit(preSum, A, L, mid, preSumDict).result()
        rightSum = executor.submit(preSum, A, mid + 1, R, preSumDict).result()
        sumLR = leftSum + rightSum
        preSumDict[(L, R)] = sumLR

        return sumLR
    

def prefixSum(A, L, R, offset, preSumDict):
    global prefixSumArray

    if not (0 <= L <= R <= len(A)):
        return
    
    if (L == R):
        prefixSumArray[L] = A[L] + offset
        return
    
    mid = (L + R) // 2
    leftSum = preSumDict[(L, mid)]

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        executor.submit(prefixSum, A, L, mid, offset, preSumDict).result()
        executor.submit(prefixSum, A, mid + 1, R, offset + leftSum, preSumDict).result()

    
if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(0, 10, size=10)
    print(f'Input array: {A}')

    preSumDict = Manager().dict()
    prefixSumArray = Array(c_int64, [0] * len(A), lock=False)

    preSum(A, 0, len(A) - 1, preSumDict)
    prefixSum(A, 0, len(A) - 1, 0, preSumDict)

    print(f'Prefix-sum array: {prefixSumArray[:]}')
