package tracked

import "context"

func (t *task) useGoalConfigVersion(ctx context.Context) error {
	runtime, err := t.getRuntime()
	if err != nil {
		return err
	}

	runtime.ConfigVersion = runtime.DesiredConfigVersion

	return t.job.m.UpdateTaskRuntime(ctx, t.job.id, t.id, runtime)
}
