from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from ctypes import c_int64
import random
import multiprocessing as mp
from multiprocessing import Array, Manager


def genRandArray(numElements, minSize, maxSize):
    nested_array = []
    for _ in range(numElements):
        size = random.randint(minSize, maxSize)
        inner_array = [random.randint(-100, 100) for _ in range(size)]
        nested_array.append(inner_array)
    return nested_array


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
    prefixSumArray = Array(c_int64, [0] * len(A), lock=False)

    prefixSum(A, 0, len(A) - 1, 0, preSumDict)

    return prefixSumArray[:]


def updateFlattendArr(vt, offset, value):
    global flattenedArr
    flattenedArr[vt + offset] = value


def calculateFlattenedArr(arrSize, offsetArr, flattenSize):
    global flattenedArr
    
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        flattenedArr = Array(c_int64, [0] * flattenSize, lock=False)
        for i in range(arrSize[0]):
            flattenedArr[i] = A[0][i]

        for i in range(1, len(A)):
            for j in range(arrSize[i]):
                executor.submit(updateFlattendArr, j, offsetArr[i - 1], A[i][j]).result()
    
    return flattenedArr[:]

def flatten(A):
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        arrSize = [0] * len(A)
        flattenSize = 0
        for i in range(len(A)):
            subArrSize = executor.submit(len, A[i]).result()
            flattenSize += subArrSize
            arrSize[i] = subArrSize

        offsetArr = getPrefixSum(arrSize)
        return calculateFlattenedArr(arrSize, offsetArr, flattenSize)
    
if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = genRandArray(5, 2, 10)
    print(f'Input array: {A}')
    print(f'Flattened array: {flatten(A)}')