package tools

import (
	"fmt"
	"testing"
)

func TestSlice(t *testing.T) {
	fmt.Println(InSlice[int](0, []int{1, 2, 3}))
	fmt.Println(InSlice[int](0, []int{1, 2, 3, 0}))
	fmt.Println(InSlice[string]("0", []string{"1", "2", "3", "0"}))
	fmt.Println(InSlice[string]("0", []string{"1", "2", "3"}))

	fmt.Println()

	fmt.Println(InSlice(0, []int64{1, 2, 3}))
	fmt.Println(InSlice(0, []int64{1, 2, 3, 0}))
	fmt.Println(InSlice("0", []string{"1", "2", "3", "0"}))
	fmt.Println(InSlice("0", []string{"1", "2", "3"}))

	// fmt.Println(InSlice("0", []int{1, 2, 3, 0}))
	// fmt.Println(InSlice(int16(0), []int64{1, 2, 3, 0}))

}

func TestIntSlice2StringSlice(t *testing.T) {
	fmt.Println(fmt.Sprintf("%+#v", IntSlice2StringSlice[int]([]int{1, 2, 3})))
	fmt.Println(fmt.Sprintf("%+#v", IntSlice2StringSlice[int64]([]int64{1, 2, 3, 0})))

	fmt.Println()

	fmt.Println(fmt.Sprintf("%+#v", IntSlice2StringSlice([]int{1, 2, 3})))
	fmt.Println(fmt.Sprintf("%+#v", IntSlice2StringSlice([]int64{1, 2, 3, 0})))

}

func TestStringSlice2IntSlice(t *testing.T) {
	fmt.Println(fmt.Sprintf("%+#v", StringSlice2IntSlice[int]([]string{"1", "2", "3"})))
	fmt.Println(fmt.Sprintf("%+#v", StringSlice2IntSlice[int64]([]string{"1", "2", "3"})))

	fmt.Println()

	// fmt.Println(fmt.Sprintf("%+#v", StringSlice2IntSlice([]string{"1", "2", "3"})))
	// fmt.Println(fmt.Sprintf("%+#v", StringSlice2IntSlice([]string{"1", "2", "3"})))

}

func TestSliceRange(t *testing.T) {
	fmt.Println(SliceRange(0, 10, 3))
}

func TestSliceSum(t *testing.T) {
	// fmt.Println(fmt.Sprintf("%+#v", SliceSum[int]([]uint64{0, 10, 3})))
	fmt.Println(fmt.Sprintf("%+#v", SliceSum([]int64{0, 10, 3})))
	fmt.Println(fmt.Sprintf("%+#v", SliceSum([]float64{0, 10, 3})))
}
