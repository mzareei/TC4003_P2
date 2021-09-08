package mapreduce

import "sync"

func getTaskArgs(mr *Master, taskNumber int, nios int, phase jobPhase) DoTaskArgs {
	var args DoTaskArgs

	args.File = mr.files[taskNumber]
	args.JobName = mr.jobName
	args.NumOtherPhase = nios
	args.TaskNumber = taskNumber
	args.Phase = phase

	return args
}

func processCall(mr *Master, wg *sync.WaitGroup, taskNumber int, nios int, phase jobPhase) {
	for {
		worker := <-mr.registerChannel
		args := getTaskArgs(mr, taskNumber, nios, phase)
		ok := call(worker, "Worker.DoTask", &args, new(struct{}))

		if ok {
			go func() {
				mr.registerChannel <- worker
			}()
			break
		}
	}

	wg.Done()
}

func processTasks(mr *Master, ntasks int, nios int, phase jobPhase) {
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		go processCall(mr, &wg, i, nios, phase)
	}
	wg.Wait() // Salir después de que se completen todas las tareas ntasks
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	processTasks(mr, ntasks, nios, phase)

	debug("Schedule: %v phase done\n", phase)
}
