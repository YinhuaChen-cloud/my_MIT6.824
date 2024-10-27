package debug

// 开头大写，才能被外部文件所见
func Assert(condition bool, message string) {
    if !condition {
        panic(message)
    }
}

