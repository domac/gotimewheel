package timewheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

//卡槽
type Slot struct {
	sync.RWMutex
	index int
	elems *list.List
}

type SlotElem struct {
	id       uint32
	task     Task
	ttl      int
	expireCh chan bool
}

//超时任务
type Task interface {
	Expire()
}

//时间轮
type TimeWheel struct {
	maxHash      uint32
	autoId       int32
	lock         sync.RWMutex
	ticker       *time.Ticker
	wheel        []*Slot
	hashWheel    map[uint32]*list.List
	tickDuration time.Duration
	ticksCount   int32
	cursor       int32
	quit         chan bool
}

//创建时间轮
func NewTimeWheel(tickDuration time.Duration, ticksCount int) *TimeWheel {
	ticker := time.NewTicker(tickDuration)
	maxhash := 50 * 10000
	tw := &TimeWheel{
		maxHash:      uint32(maxhash),
		lock:         sync.RWMutex{},
		tickDuration: tickDuration,
		ticker:       ticker,
		ticksCount:   int32(ticksCount),
		cursor:       0,
		wheel:        buildWheel(ticksCount),
		hashWheel:    make(map[uint32]*list.List, maxhash),
		quit:         make(chan bool),
	}
	return tw
}

func (t TimeWheel) Start() {
	//并行开启超时调度
	go func() {
		for {
			select {
			case <-t.ticker.C:
				t.cursor++
				if t.cursor == int32(t.ticksCount) {
					t.cursor = 0
				}
				t.notifyExpiredTimeOut(t.cursor)
			case <-t.quit:
				println("time wheel is closing")
				return
			}
		}
	}()
}

func (t TimeWheel) Stop() {
	t.quit <- true
	t.ticker.Stop()
}

//构建时间轮结构
func buildWheel(ticksCount int) []*Slot {
	wheelSlots := make([]*Slot, 0, ticksCount)
	for i := 0; i < ticksCount; i++ {
		wheelSlots = append(wheelSlots, func() *Slot {
			return &Slot{
				index: i,
				elems: list.New()}
		}())
	}
	return wheelSlots
}

//超时过期处理
func (t *TimeWheel) notifyExpiredTimeOut(index int32) {
	var remove []*list.Element

	t.lock.RLock()
	slots := t.wheel[index]
	slots.RLock()
	for e := slots.elems.Back(); nil != e; e = e.Prev() {
		se := e.Value.(*SlotElem)
		se.ttl--
		if se.ttl <= 0 {
			//加入待删除队列
			if remove == nil {
				remove = make([]*list.Element, 0, 10)
			}
			remove = append(remove, e)
			se.expireCh <- true

			//异步回调任务的过期处理函数
			go func() {
				se.task.Expire()
			}()
		}
	}

	slots.RUnlock()
	t.lock.RUnlock()

	//如果待删队列中不为空，则进行元素删除
	if len(remove) > 0 {
		slots.Lock()

		//先清除槽中队列的元素
		for _, v := range remove {
			slots.elems.Remove(v)
		}
		slots.Unlock()

		//删除本地哈希队列记录
		t.lock.Lock()
		for _, v := range remove {
			//找hashId先
			elemTask := v.Value.(*SlotElem)
			hashId := t.hashId(elemTask.id)
			hashWheelQueue, ok := t.hashWheel[hashId]
			if ok {
				//从头到尾删除
				for e := hashWheelQueue.Front(); nil != e; e = e.Next() {
					if e.Value.(*list.Element) == v {
						hashWheelQueue.Remove(e)
						select {
						case <-elemTask.expireCh:
						default:
						}
						break
					}
				}
			}
		}
		t.lock.Unlock()
	}
}

//添加一个轮子超时任务
func (t *TimeWheel) AddTask(timeout time.Duration, mytask Task) (int64, chan bool) {
	slotIndex := t.getPreTickIndex() + 1

	if slotIndex >= t.ticksCount {
		slotIndex = slotIndex - t.ticksCount
	}

	ttl := int(int64(timeout) / (int64(t.tickDuration) * int64(t.ticksCount-1)))
	tid := t.taskId(slotIndex)
	expireCh := make(chan bool, 1)
	job := &SlotElem{
		id:       tid,
		task:     mytask,
		ttl:      ttl,
		expireCh: expireCh}

	t.lock.Lock()
	slots := t.wheel[slotIndex]
	slots.Lock()
	e := slots.elems.PushFront(job)
	slots.Unlock()

	hashId := t.hashId(tid)
	v, ok := t.hashWheel[hashId]
	if ok {
		t.hashWheel[hashId].PushBack(e)
	} else {
		v = list.New()
		v.PushBack(e)
		t.hashWheel[hashId] = v
	}
	t.lock.Unlock()
	return int64(tid), job.expireCh
}

//删除一个超时任务
func (t *TimeWheel) RemoveTask(taskId int64) {
	tid := uint32(taskId)
	sid := t.decodeSlot(taskId)

	t.lock.RLock()
	slots := t.wheel[sid]
	slotIndex := t.hashId(tid)
	link, ok := t.hashWheel[slotIndex]

	if ok {
		slots.Lock()
		var tmp *list.Element
		for e := link.Front(); nil != e; e = e.Next() {
			tmp = e.Value.(*list.Element)
			job := tmp.Value.(*SlotElem)
			if job.id == tid {
				link.Remove(e)
				slots.elems.Remove(tmp)
				break
			}
		}
		slots.Unlock()
	}
	t.lock.RUnlock()
}

func (t *TimeWheel) hashId(id uint32) uint32 {
	return (id % t.maxHash)
}

func (t *TimeWheel) taskId(index int32) uint32 {
	return uint32(index<<32 | atomic.AddInt32(&t.autoId, 1))
}

func (t *TimeWheel) decodeSlot(taskId int64) uint32 {
	return uint32(taskId) >> 32
}

func (t *TimeWheel) getPreTickIndex() int32 {
	idx := atomic.LoadInt32(&t.cursor)
	if idx > 0 {
		idx -= 1
	} else {
		idx = (t.ticksCount - 1)
	}
	return idx
}
