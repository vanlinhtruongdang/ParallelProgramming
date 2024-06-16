from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from ctypes import c_byte, c_int
import multiprocessing as mp
from multiprocessing import Array, Manager
import numpy as np

def F(A, pos, pivot):
    global filteredArr

    if A[pos] < pivot:
        filteredArr[pos] = 1
    else:
        filteredArr[pos] = 0


def filtering(A, pivot):
    global filteredArr
    filteredArr = Array(c_byte, len(A), lock=False)

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        for pos in range(len(A)):
            executor.submit(F, A, pos, pivot).result()

        return filteredArr[:]


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


def getPrefixSum(A):
    global prefixSumArray

    _, preSumDict = preSum(A, 0, len(A) - 1)
    prefixSumArray = Array(c_int, [0] * len(A), lock=False)

    prefixSum(A, 0, len(A) - 1, 0, preSumDict)

    return prefixSumArray[:]


def check(A, pos, prefixSumSatisfy, prefixSumNonSatisfy):
    global satisfyArr
    global nonSatisfyArr

    if prefixSumSatisfy[pos] != prefixSumSatisfy[pos - 1]:
        satisfyArr[prefixSumSatisfy[pos] - 1] = A[pos]
    
    if prefixSumNonSatisfy[pos] != prefixSumNonSatisfy[pos - 1]:
        nonSatisfyArr[prefixSumNonSatisfy[pos] - 1] = A[pos]


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

def indexMinus(A, size):
    result = [0] * size
    for i in range(size):
        result[i] = i - A[i] + 1
    
    return result
        

def partition(A, pivot):
    filteredArr = filtering(A, pivot)
    prefixSumSatisfy = getPrefixSum(filteredArr)    
    prefixSumNonSatisfy = indexMinus(prefixSumSatisfy, len(prefixSumSatisfy))

    left, right = packing(A, prefixSumSatisfy, prefixSumNonSatisfy)

    return left, right


def quicksort(A):
    if len(A) <= 1:
        return A
        
    pivot = A[0]
    left, right = partition(A[1:], pivot)

    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        return executor.submit(quicksort, left).result() + [pivot] + executor.submit(quicksort, right).result()

if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(-100, 100, size=5)
    print(f'Input array: {A}')

    sortedA = quicksort(A)
    print(f'Sorted array: {sortedA}')
