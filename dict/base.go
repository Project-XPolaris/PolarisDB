package dict

type Dict[T interface{}] struct {
	Data map[string]T
}

func NewDict[T interface{}]() *Dict[T] {
	return &Dict[T]{
		Data: make(map[string]T),
	}
}

func (d *Dict[T]) Add(key string, value T) {
	d.Data[key] = value
}
func (d *Dict[T]) AddOrFind(key string, value T) T {
	if _, ok := d.Data[key]; !ok {
		d.Data[key] = value
	}
	return d.Data[key]
}
func (d *Dict[T]) Replace(key string, value T) {
	d.Data[key] = value
}
func (d *Dict[T]) Delete(key string) {
	delete(d.Data, key)
}

func (d *Dict[T]) Find(key string) (T, bool) {
	value, ok := d.Data[key]
	return value, ok
}

func (d *Dict[T]) RandomKey() string {
	for key := range d.Data {
		return key
	}
	return ""
}

func (d *Dict[T]) Len() int {
	return len(d.Data)
}

func (d *Dict[T]) Keys() []string {
	keys := make([]string, 0, len(d.Data))
	for key := range d.Data {
		keys = append(keys, key)
	}
	return keys
}
