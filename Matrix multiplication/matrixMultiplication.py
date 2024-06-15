from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
import multiprocessing as mp

def matrixMultiply(A, B):
    n = len(A)

    if n == 1:
        return [[A[0][0] * B[0][0]]]
    
    A11 = [row[:n//2] for row in A[:n//2]]
    A12 = [row[n//2:] for row in A[:n//2]]
    A21 = [row[:n//2] for row in A[n//2:]]
    A22 = [row[n//2:] for row in A[n//2:]]
    
    B11 = [row[:n//2] for row in B[:n//2]]
    B12 = [row[n//2:] for row in B[:n//2]]
    B21 = [row[:n//2] for row in B[n//2:]]
    B22 = [row[n//2:] for row in B[n//2:]]
    
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        C11 = executor.submit(matrixAdd, 
                              executor.submit(matrixMultiply, A11, B11).result(), 
                              executor.submit(matrixMultiply, A12, B21).result()).result()

        C12 = executor.submit(matrixAdd, 
                              executor.submit(matrixMultiply, A11, B12).result(),
                              executor.submit(matrixMultiply, A12, B22).result()).result()
        
        C21 = executor.submit(matrixAdd,
                             executor.submit(matrixMultiply, A21, B11).result(),
                             executor.submit(matrixMultiply, A22, B21).result()).result()
        
        C22 = executor.submit(matrixAdd,
                              executor.submit(matrixMultiply, A21, B12).result(),
                              executor.submit(matrixMultiply, A22, B22).result()).result()
        
        C = []
        for row in range(n):
            if row < n//2:
                C.append(C11[row] + C12[row])
            else:
                C.append(C21[row-n//2] + C22[row-n//2])

        return C

def matrixAdd(A, B):
    return [[a + b for a, b in zip(row_a, row_b)] for row_a, row_b in zip(A, B)]


if __name__ == '__main__':
    mp.set_start_method('fork', force=True)

    A = np.random.randint(0, 10, size=(2, 2))
    B = np.random.randint(0, 10, size=(2, 2))
    print(f'Matrix A \n {A}')
    print(f'Matrix B \n {B}')

    C = matrixMultiply(A, B)
    C = np.array(C)

    print(f'C = A x B \n {C}')

