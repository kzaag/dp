package cmn

import (
	"fmt"
	"os"
)

/*
	those functions are meant to help with printing and formatting text.
	But they may not last for too long since they are nested to the oblivion.
	Solution would be to use macros and preprocessor.
	But guess what
	golang compiler doesnt support that either.
	...
	we will see
*/

/*
	applies Ansi formatting to the text and at the end resets it
*/
func FPrintfTrailing(f *os.File, seq AnsiFlag, format string, args ...interface{}) {
	fmt.Fprintf(
		f,
		fmt.Sprintf("%v%s%v", seq, format, AttrOff),
		args...)
}

/*
	works the same as PrintfTrailing, but adds LF before finishing escape sequence
*/
func FPrintflnTrailing(f *os.File, seq AnsiFlag, format string, args ...interface{}) {
	fmt.Fprintf(
		f,
		fmt.Sprintf("%v%s\n%v", seq, format, AttrOff),
		args...)
}

func PrintflnSuccess(_fmt string, argv ...interface{}) {
	FPrintflnTrailing(os.Stdout, ForeGreen, _fmt, argv...)
}

func PrintflnError(_fmt string, argv ...interface{}) {
	FPrintflnTrailing(os.Stderr, ForeRed, _fmt, argv...)
}

func PrintError(err error) {
	PrintflnError("%s", err)
}

func PrintflnWarn(_fmt string, argv ...interface{}) {
	FPrintflnTrailing(os.Stderr, ForeYellow, _fmt, argv...)
}

func PrintflnNotify(_fmt string, argv ...interface{}) {
	FPrintflnTrailing(os.Stdout, ForeBlue, _fmt, argv...)
}

/*
	conditional formatting.
	if fmtdisable == false then formatting provided function fptr will be used
	else raw call is equivalent to calling fmt.printf with additional LF at the end
*/
func CndPrintfln(
	fmtdisable bool,
	fptr func(string, ...interface{}),
	_fmt string, argv ...interface{}) {

	if fmtdisable {
		fmt.Printf(fmt.Sprintf("%s\n", _fmt), argv...)
	} else {
		fptr(_fmt, argv...)
	}
}

func CndPrintln(
	fmtdisable bool,
	fptr func(string, ...interface{}),
	text string) {

	if fmtdisable {
		fmt.Println(text)
	} else {
		fptr("%s", text)
	}
}

func CndPrintError(fmtdisable bool, err error) {
	if fmtdisable {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	} else {
		PrintError(err)
	}
}
