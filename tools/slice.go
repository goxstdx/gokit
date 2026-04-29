package tools

import (
	"cmp"
	"math/rand"
	"sort"
	"time"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type reducetype[T any] func(T) T
type filtertype[T any] func(T) bool

// InSlice checks given string in string slice or not.
func InSlice[T comparable](value T, slice []T) bool {
	if len(slice) == 0 {
		return false // 明确处理空切片的情况
	}

	for _, element := range slice {
		if element == value {
			return true
		}
	}
	return false
}

// SliceRandList generate an int slice from min to max.
func SliceRandList(min, max int) []int {
	if max < min {
		min, max = max, min
	}
	length := max - min + 1
	t0 := time.Now()
	rand.Seed(int64(t0.Nanosecond()))
	list := rand.Perm(length)
	for index := range list {
		list[index] += min
	}
	return list
}

// SliceMerge 合并
func SliceMerge[T any](slice1, slice2 []T) (c []T) {
	c = append(slice1, slice2...)
	return
}

// SliceReduce 循环处理
func SliceReduce[T any](slice []T, a reducetype[T]) (dslice []T) {
	for _, v := range slice {
		dslice = append(dslice, a(v))
	}
	return
}

// SliceRand 随机返回一个
func SliceRand[T any](a []T) (b T) {
	randnum := rand.Intn(len(a))
	b = a[randnum]
	return
}

// SliceSum 所有值求和
func SliceSum[T constraints.Integer | constraints.Float](intslice []T) (sum T) {
	for _, v := range intslice {
		sum += v
	}
	return
}

// SliceFilter 循环过滤
func SliceFilter[T any](slice []T, a filtertype[T]) (ftslice []T) {
	for _, v := range slice {
		if a(v) {
			ftslice = append(ftslice, v)
		}
	}
	return
}

// SliceDiff 差集
func SliceDiff[T comparable](slice1, slice2 []T) (diffslice []T) {
	for _, v := range slice1 {
		if !InSlice(v, slice2) {
			diffslice = append(diffslice, v)
		}
	}
	return
}

// SliceIntersect 取交集
func SliceIntersect[T comparable](slice1, slice2 []T) (diffslice []T) {
	for _, v := range slice1 {
		if InSlice(v, slice2) {
			diffslice = append(diffslice, v)
		}
	}
	return
}

// SliceChunk 将 slice 按 size 分割
func SliceChunk[T any](slice []T, size int) (chunkslice [][]T) {
	if size >= len(slice) {
		chunkslice = append(chunkslice, slice)
		return
	}
	end := size
	for i := 0; i <= (len(slice) - size); i += size {
		chunkslice = append(chunkslice, slice[i:end])
		end += size
	}
	return
}

// SliceRange start->end 循环步进 step
// step 负数好像有问题
func SliceRange(start, end, step int64) (intslice []int64) {
	for i := start; i <= end; i += step {
		intslice = append(intslice, i)
	}
	return
}

// SlicePad 将 slice 填充 val 到 size 的长度
func SlicePad[T any](slice []T, size int, val T) []T {
	if size <= len(slice) {
		return slice
	}
	for i := 0; i < (size - len(slice)); i++ {
		slice = append(slice, val)
	}
	return slice
}

// SliceUnique 去掉重复的值
func SliceUnique[T comparable](slice []T) (uniqueslice []T) {
	for _, v := range slice {
		if !InSlice(v, uniqueslice) {
			uniqueslice = append(uniqueslice, v)
		}
	}
	return
}

// SliceShuffle 数组值打乱
func SliceShuffle[T any](slice []T) []T {
	for i := 0; i < len(slice); i++ {
		a := rand.Intn(len(slice))
		b := rand.Intn(len(slice))
		slice[a], slice[b] = slice[b], slice[a]
	}
	return slice
}

// SliceRepeatVal 获取数组中重复的值
func SliceRepeatVal[T comparable](slice []T) (repeatSlice []T) {
	length := len(slice)
	for i := 0; i < length; i++ {
		for j := i + 1; j < length; j++ {
			if slice[i] == slice[j] {
				repeatSlice = append(repeatSlice, slice[i])
			}
		}
	}
	return
}

func StringSlice2IntSlice[T constraints.Integer](in []string) (out []T) {
	for _, str := range in {
		vv, _ := Str2Integer[T](str)
		out = append(out, T(vv))
	}
	return
}

func IntSlice2StringSlice[T constraints.Integer](in []T) (out []string) {
	for _, str := range in {
		vv := Integer2Str(str)
		out = append(out, vv)
	}
	return
}

// SortAndUniqueIntegerCopy 对整数或浮点数切片进行排序并去重，返回一个新切片
func SortAndUniqueIntegerCopy[T constraints.Integer | constraints.Float](nums []T) []T {
	if len(nums) == 0 {
		return nil
	}

	cp := slices.Clone(nums)

	sort.Slice(
		cp, func(i, j int) bool {
			return cmp.Less(cp[i], cp[j])
		},
	)

	j := 1
	for i := 1; i < len(cp); i++ {
		if cp[i] != cp[i-1] {
			cp[j] = cp[i]
			j++
		}
	}

	return cp[:j]
}
