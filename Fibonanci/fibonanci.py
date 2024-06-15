import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

def Fibonanci(n):
    if (n < 0):
        return None
    if (n == 0):
        return 0
    if (n <= 2):
        return 1
    
    # with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        k = n // 2
        Fi_k = executor.submit(Fibonanci, k).result()
        Fi_k1 = executor.submit(Fibonanci, k + 1).result()

        return Fi_k * (2 * Fi_k1 - Fi_k) if (n % 2 == 0) else Fi_k1**2 + Fi_k**2


if __name__ == "__main__":
    mp.set_start_method('fork', force=True)

    n = 10
    nFibo = Fibonanci(n)
    print(f'{n}-th Fibonanci number: {nFibo}')