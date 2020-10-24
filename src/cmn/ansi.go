package cmn

import (
	"fmt"
	"unsafe"
)

/*
	some misc utils related to text formatting and pretty printing

	****example simple usage****

	print red text

	fmt.Printf("%vHello World%v\n", cmn.ForeRed, cmn.AttrOff)

	****example complex usage****

	print dazzling text

	fmt.Printf(
		"%v%s%v%s%v%v%s%v%s%v\n",
		cmn.AttrUnderscore|cmn.ForeCyan,
		"daz",
		cmn.BackCyan|cmn.ForeRed,
		"zle",
		cmn.AttrOff, // reset attributes (remove underscore)
		cmn.AttrBold|cmn.ForeMagenta,
		" me",
		cmn.ForeGreen|cmn.AttrReverseVideo,
		"!!!",
		cmn.AttrOff)

*/

type AnsiFlag uint32

func AnsiFlagCompose(
	attr AnsiFlag,
	fore AnsiFlag,
	back AnsiFlag) AnsiFlag {

	// encoded bytes in memory : (uint32)
	// [0 | back | fore | attr]
	return AnsiFlag(attr) + AnsiFlag(fore<<1) + AnsiFlag(back<<2)
}

const (
	AttrOff AnsiFlag = iota
	AttrBold
	_
	_
	AttrUnderscore
	AttrBlink
	_
	AttrReverseVideo
	AttrConcealed
)

const (
	ForeBlack AnsiFlag = (iota + 30) << 8
	ForeRed
	ForeGreen
	ForeYellow
	ForeBlue
	ForeMagenta
	ForeCyan
	ForeWhite
)

const (
	BackBlack AnsiFlag = (iota + 40) << 16
	BackRed
	BackGreen
	BackYellow
	BackBlue
	BackMagenta
	BackCyan
	BackWhite
)

func (f AnsiFlag) String() string {
	var parts [unsafe.Sizeof(f)]AnsiFlag
	var ix int = 0
	var tmp AnsiFlag
	var written bool = false

	for ix < len(parts) {
		if tmp = f & 0xFF; tmp != 0 {
			parts[ix] = tmp
		}
		f >>= 8
		ix++
	}

	var ret string = "\033["
	for ix = 0; ix < len(parts); ix++ {
		if parts[ix] == 0 {
			continue
		}
		if !written {
			written = true
			ret = fmt.Sprintf("%s%d", ret, parts[ix])
		} else {
			ret = fmt.Sprintf("%s;%d", ret, parts[ix])
		}
	}
	if !written {
		ret = fmt.Sprintf("%s0", ret)
	}

	return fmt.Sprintf("%sm", ret)
}
