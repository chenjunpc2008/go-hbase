package hbasepool

import (
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
)

func Test_timeSub(t *testing.T) {

    var (
        t1, t2      time.Time
        tgap        time.Duration
        IdleTimeOut = 15 * time.Second
    )

    t1 = time.Now()

    time.Sleep(20 * time.Second)

    t2 = time.Now()

    tgap = t2.Sub(t1)
    if IdleTimeOut < tgap {
        assert.Equal(t, true, true)
    } else {
        assert.Equal(t, true, false)
    }
}

func Test_idleFront(t *testing.T) {
    var (
        idle = make([]int, 0)
    )

    //
    for i := 0; i < 10; i++ {
        idle = append(idle, i)
    }

    pick := idle[0]
    assert.Equal(t, 0, pick)

    idle = idle[1:]
    for i := 0; i < 9; i++ {
        assert.Equal(t, i+1, idle[i])
    }

    //
    idle = []int{1}

    pick = idle[0]
    assert.Equal(t, 1, pick)

    idle = idle[1:]
    assert.Equal(t, 0, len(idle))
}
