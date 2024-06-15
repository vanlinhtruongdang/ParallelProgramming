from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing as mp
import numpy as np

def Sum(A, L, R):
    if not (0 <= L <= R <= len(A)):
        return None
    
    if (L == R):
        return A[L]
    
    mid = (L + R) // 2
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        leftSum = executor.submit(Sum, A, L, mid).result()
        rightSum = executor.submit(Sum, A, mid + 1, R).result()
        sumLR = leftSum + rightSum
        
        return sumLR
    
if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(0, 10, size=10)
    print(f'Input array: {A}')

    L = 0
    R = len(A) - 1
    sumLR = Sum(A, L, R)
    print(f'Sum({L}-{R}): {sumLR}')