package scalar

// Resources represents a scalar resource having CPU, Memory, Disk and GPU
type Resources interface {
	GetCPU() float64
	GetMem() float64
	GetDisk() float64
	GetGPU() float64
}
