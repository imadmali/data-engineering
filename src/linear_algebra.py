from functools import reduce
import numpy as np
from scipy.sparse import csc_matrix

## Sequences
print('\nSequences')
# Sequence defined by steps
result = np.arange(0,1,0.1)
print(result)
# Sequence defined array size
result = np.linspace(0,0.9,10)
print(result)

## Identity matrix
print('\nIdentify matrix')
I = np.eye(3)
print(I)

## Repeat an array
print('\nRepeat an array')
result = np.tile(3, (2,2))
print(result)
result = np.tile([1,2], (2,3))
print(result)

## Sparse matrix
print('\nSparse matrix')
X = np.array([[0,0,1],[0,1,1],[1,0,1]])
xi = np.nonzero(X)
X_sparse = csc_matrix((X[xi], xi))

print('X\n', X)
print('X_sparse\n', X_sparse)

# recover original matrix
X_sparse.toarray()

## Matrix sum
print('\nMatrix sum')
N = np.array([[1,2],[3,4]])

result = N + 1
print(result)
result = N + np.array([1,2])
print(result)

## Matrix sum over axis
print('\nMatrix sum over axis')
N = np.array([[1,2],[3,4]])

# Sum all elements
result = np.sum(N)
print(result)
# Sum over first axis
result = np.sum(N, 0)
print(result)
# Sum over second axis
result = np.sum(N, 1)
print(result)

## Matrix scalar product
print('\nMatrix scalar product')
X = np.array([[1,2,3],[4,5,6],[7,8,9]])
a = 2
result = X * a
print(result)

## Matrix transpose
print('\nMatrix transpose')
X = np.array([[1,2],[3,4],[5,6]])
result = np.transpose(X)
print(result)

## Matrix product
# You can also use np.matmul but broadcasting rules are different
print('\nMatrix product')
N = np.array([[1,2],[3,4]])
M = np.array([2,2])

result = np.dot(N, M)
print(result)
result = np.dot(N, np.transpose(X))
print(result)

## Dot product
print('\nDot product')
x = np.array([1,2,3])
y = np.array([1,2,3])

result = np.dot(x, y)
print(result)

## Outer product
print('\nOuter product')
N = np.array([[1,2],[3,4],[5,6]])
M = np.array([0,1])
np.outer(N,M)

## Tensor dot product
print('\nTensor dot product')

## Kronecker product
print('\nKronecker product')
I = np.eye(3)
O = np.ones((2,2))
result = np.kron(I,O)
print(result)

## QR decomposition
print('\nQR decomposition')
X = np.array([[1,2],[3,4],[5,6]])

q, r = np.linalg.qr(X)
print('q\n', q)
print('r\n', r)

# recover original matrix
np.dot(q,r)

## Singular value decomposition
print('\nSingular value decomposition')
X = np.array([[1,2],[3,4],[5,6]])

u, s, vh = np.linalg.svd(X)
print('u\n', u)
print('s\n', s)
print('vh\n', vh)

# recover original matrix
S = np.vstack([np.diag(s), np.zeros(2)])
reduce(np.dot, [u, S, vh])

## Norm
x = np.array([1,2,3])

result = np.linalg.norm(x)
print(result)
