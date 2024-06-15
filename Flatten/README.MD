# GIẢI THUẬT PARALLEL FLATTEN

**Ý tưởng chính**: để làm phẳng một mảng gộp $A$ với từng phần tử là một mảng con 1 chiều có kích thước khác nhau một cách song song, ta cần biết được vị trí bắt đầu của từng mảng con ($offset$) trong mảng kết quả $B$. Gọi mảng $S$ là tập hợp kích thước của các mảng con trong $A$, ta có thể thực hiện tính toán $offset$ song song bằng cách áp dụng thuật toán Parallel Prefix-Sum lên mảng này nhằm khởi tạo mảng tổng tiền tố của $S$. Khi đó, mảng $B$ sẽ được cập nhật dựa trên công thức:

$$
\begin{aligned}
    B[j + offset[i-&1]] = A[i][j] \\
    \forall i \in [1, length(A)&], \forall j \in [1, S[i]]
\end{aligned}
$$


**Mã giả**:
```actionscript
function Flatten(A):v
    in parallel: 
        for i = 1 to length(A): S[i] = size(A[i])
    
    offset = parallelPrefixSum(S)
    
    in parallel:
        for i = 1 to length(A):
            for j = 1 to S[i]:
                B[j + offset[i-1]] = A[i][j]

    return B
```

**Giải thích**: hàm `Flatten(A)` có chức năng làm phẳng mảng gộp $A$. Đầu tiên, ta khởi tạo mảng $S$ bằng cách lấy kích thước từng mảng con của $A$ một cách song song. Tiếp theo, hàm `parallelPrefixSum` được sử dụng để tính toán song song mảng $offset$ nhằm tìm được các vị trí bắt đầu của từng mảng con trong mảng kết quả $B$. Cuồi cùng, mảng $B$ được cập nhật bằng hai lần duyệt song song dựa theo công thức trên.

**Ví dụ minh hoạ**:
![alt text](flatten.png)