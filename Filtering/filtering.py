from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from ctypes import c_byte, c_int
from multiprocessing import Array, Manager
import multiprocessing as mp
import numpy as np

def F(A, pos):
    global filteredArr

    if A[pos] % 2 == 0:
        filteredArr[pos] = 1
    else:
        filteredArr[pos] = 0


def filtering(A):
    global filteredArr
    filteredArr = Array(c_byte, len(A), lock=False)

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        for pos in range(len(A)):
            executor.submit(F, A, pos).result()

        return filteredArr[:]
    

def preSum(A, L, R, preSumDict):    
    if (L > R or L < 0 or R == len(A)):
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

    if (L > R or L < 0 or R == len(A)):
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


def getPrefixSum(A):
    global prefixSumArray

    preSumDict = Manager().dict()
    prefixSumArray = Array(c_int, [0] * len(A), lock=False)

    preSum(A, 0, len(filteredArr) - 1, preSumDict)
    prefixSum(A, 0, len(filteredArr) - 1, 0, preSumDict)

    return prefixSumArray[:]


def check(A, pos, prefixSumSatisfy, prefixSumNonSatisfy):
    global satisfyArr
    global nonSatisfyArr

    if prefixSumSatisfy[pos] != prefixSumSatisfy[pos - 1]:
        satisfyArr[prefixSumSatisfy[pos] - 1] = A[pos]
    
    if prefixSumNonSatisfy[pos] != prefixSumNonSatisfy[pos - 1]:
        nonSatisfyArr[prefixSumNonSatisfy[pos] - 1] = A[pos]

def indexMinus(A, size):
    result = [0] * size
    for i in range(size):
        result[i] = i - A[i] + 1
    
    return result


def packing(A, prefixSumSatisfy, prefixSumNonSatisfy):
    global satisfyArr
    global nonSatisfyArr

    satisfyArr = Array(c_int, [0] * prefixSumSatisfy[-1], lock=False)
    nonSatisfyArr = Array(c_int, [0] * prefixSumNonSatisfy[-1], lock=False)


    if prefixSumSatisfy[0] == 1:
        satisfyArr[0] = A[0]

    if prefixSumNonSatisfy[0] == 1:
        nonSatisfyArr[0] = A[0]

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        for i in range(1, len(prefixSumSatisfy)):
            executor.submit(check, A, i, prefixSumSatisfy, prefixSumNonSatisfy)
    
    return satisfyArr[:], nonSatisfyArr[:]
        

if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(low=0, high=100, size=10)
    print(f'Input array: {A}')

    filteredArr = filtering(A)
    prefixSumSatisfy = getPrefixSum(filteredArr)    
    prefixSumNonSatisfy = indexMinus(prefixSumSatisfy, len(prefixSumSatisfy))
    satisfyArr, nonSatisfyArr = packing(A, prefixSumSatisfy, prefixSumNonSatisfy)

    print(f'Filtered array: {filteredArr[:]}')
    print(f'Prefix-Sum satisfy filter array: {prefixSumSatisfy}')
    print(f'Prefix-Sum non-satisfy filter array: {prefixSumNonSatisfy}')
    print(f'Satisfy F: {satisfyArr}')
    print(f'Non-Satisfy F: {nonSatisfyArr}')


