from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing as mp
import numpy as np

def upperBound(A, value):
    L = 0
    R = len(A) - 1
    
    while L <= R:
        mid = (L + R) // 2
        if A[mid] <= value:
            L = mid + 1
        else:
            R = mid - 1
    
    return L


def merge(A, B):
    A, B = list(A), list(B)

    if len(A) == 0:
        return B
    
    if len(B) == 0:
        return A
    
    if len(A) == 1 and len(B) == 1:
        if A[0] < B[0]:
            return A + B
        else:
            return B + A
    
    mid = len(A) // 2
    insertPos = upperBound(B, A[mid])
    
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        left = executor.submit(merge, A[:mid], B[:insertPos]).result()
        right = executor.submit(merge, A[mid + 1:], B[insertPos:]).result()
        return left + [A[mid]] + right
    

def mergeSort(A):
    if len(A) <= 1:
        return A
    
    mid = len(A) // 2
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        left = executor.submit(mergeSort, A[:mid]).result()
        right = executor.submit(mergeSort, A[mid:]).result()
        return merge(left, right)

if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(-100, 100, size=10)
    print(f'Input array: {A}')

    sortedA = mergeSort(A)
    print(f'Sorted array: {sortedA}')