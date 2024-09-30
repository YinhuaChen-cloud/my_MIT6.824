package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// 定义一个类型名为 ByKey。这是一个 KeyValue 切片，也可以叫数组/列表
// for sorting by key.
type ByKey []mr.KeyValue

// 实现 ByKey 排序所需的三个函数 Len，Swap，Less
// 这三个函数分别作用：返回 ByKey 长度，交换 ByKey 中两个元素位置，判断 ByKey 两个元素的大小
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 调用命令： ../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
func main() {
	// 判断参数是否合法 (达到 3 个)
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 加载 .so 文件，注意：loadPlugin 是用户自定义函数
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	// intermediate变量是一个空的mr.KeyValue切片。
	intermediate := []mr.KeyValue{}
	// 对输入文件列表进行循环 (pg*txt 是一堆文本文件)
	for _, filename := range os.Args[2:] {
		// fmt.Printf("filename = %s\n", filename)
		// 打开输入列表里的文本文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 读取文本文件
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
// 这个函数加载动态库，动态库路径由 filename 给出
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开动态库
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// 获取动态库中名为 Map 的函数
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	// 类型断言：将查找到的 Map 函数转换为特定的函数类型 func(string, string) []mr.KeyValue。
	// 如果没有类型断言，函数指针的类型将是 interface{}，你将无法直接调用这些函数，因为 Go 不允许直接通过 interface{} 调用方法。
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	// 获取动态库中名为 Reduce 的函数
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	// 类型断言：将查找到的 Map 函数转换为特定的函数类型 func(string, string) []mr.KeyValue。
	// 如果没有类型断言，函数指针的类型将是 interface{}，你将无法直接调用这些函数，因为 Go 不允许直接通过 interface{} 调用方法。
	reducef := xreducef.(func(string, []string) string)
	// 返回 Map 和 Reduce 两个函数指针
	return mapf, reducef
}
