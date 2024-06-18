# GIẢI THUẬT PARALLEL MERGESORT

**Ý tưởng chính**: để thực hiện sắp xếp trộn trên đoạn $[L, R]$ của mảng $A$ một cách song song, ta liên tục song song chia nhỏ đoạn thành hai mảng con bên trái và bên phải cho đến khi chúng chỉ còn chứa 1 phần tử duy nhất. Cuối cùng, tất cả các đoạn con sẽ được sắp xếp hợp nhất để tạo thành mảng kết quả. Để tối ưu hoá cho việc hợp nhất, ta áp dụng chia để trị song song kết hợp cùng giải thuật tìm kiếm nhị phân và phần tử trung vị.

**Mã giả**:
```actionscript
function merge(A, B):
    if length(A) = 0:
        return B
    
    if length(B) = 0:
        return A
    
    if length(A) == 1 and length(B) == 1:
        if A[1] < B[1]:
            return concatenate(A[1], B[1])
        else:
            return concatenate(B[1], A[1])
    
    mid = length(A) / 2
    insertPos = upperBound(B, A[mid])

    leftA = getArr(A, 1, mid - 1)
    rightA = getArr(A, mid + 1, length(A))
    leftB = getArr(B, 1, insertPos)
    rightB = getArr(A, insertPos + 1, length(B))

    leftMerged = fork merge(leftA, leftB)
    rightMerged = fork merge(rightA, rightB)

    join leftSorted and rightSorted

    return concatenate(leftMerged, A[mid], rightMerged)


function mergeSort(A):
    if length(A) == 1:
        return A

    mid = length(A) / 2
    left = getArr(A, 1, mid)
    right = getArr(A, mid + 1, length(A))

    leftSorted = fork mergeSort(left)
    rightSorted = fork mergeSort(right)

    join leftSorted and rightSorted

    return merge(leftSorted, rightSorted)
```

**Giải thích**: hàm `mergeSort(A)` có tính năng thực hiện sắp xếp trộn mảng $A$. Xét điều kiện cơ sở, ta sẽ trả về giá trị $A[1]$ nếu mảng chỉ chứa 1 phần tử. Tiến hành phân tách thành 2 mảng con dựa vào chỉ số `mid = length(A) / 2`, ta có nhánh bên trái `left = getArr(A, 1, mid)` nằm trong đoạn $[1, mid]$ và nhánh bên phải `right = getArr(A, mid + 1, length(A))` nằm trong đoạn $[mid + 1, length(A)]$. Tiếp theo, ta gọi đệ quy song song hàm `mergeSort` tương ứng trên hai nhánh con và tổng hợp trộn kết quả với hàm `merge` khi tất cả các tác vụ đã hoàn thành. Về hàm `merge(A, B)`, xét các điều kiện cơ sở:
* Nếu mảng $A$ rỗng: trả về mảng $B$.
* Nếu mảng $B$ rỗng: trả về mảng $A$.
* Nếu mảng $A$ và $B$ đều có 1 phần tử: ghép nối $(A[1], B[1])$ nếu $A[1] > B[1]$ và $(B[1], A[1])$ trong trường hợp ngược lại.

Ta tìm phần tử trung vị của mảng A với chỉ số `mid = length(A) / 2` và thực hiện tìm kiếm nhị phân `upperBound` (tìm vị trí đầu tiên lớn hơn giá trị cho trước) trên mảng $B$ với giá trị $A[mid]$. Sau đó, ta chia mảng $A$ thành hai phần bên trái ($\lt A[mid]$) và bên phải ($\gt A[mid]$), tương tự với mảng $B$ nhưng phần bên trái sẽ thay đổi điều kiện ($\le A[mid]$). Cuối cùng, ta gọi đệ quy song song hàm `merge` với đầu vào là các phần con tương ứng nhau của $A$ và $B$ (`leftMerged = fork merge(leftA, leftB)` và `rightMerged = fork merge(rightA, rightB)`). Kết quả trộn được tổng hợp bằng việc ghép nối lần lượt $leftMerged, A[mid]$ và $rightMerged$ sau khi các tác vụ đã hoàn thành.

**Độ phức tạp**:

$$
\begin{aligned}
    Work&: O(nlogn) \\
    Depth&: O(log^3n)
\end{aligned}
$$


**Ví dụ minh hoạ**:
![alt text](mergeSort.png)