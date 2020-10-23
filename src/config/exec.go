package config

import "fmt"

/*
	Just grouped config array entries.
*/
type ExecCtx struct {
	Exec []*Exec
	Auth *Auth
	Name string
}

func ExecCtxNew(configuration *Data) (map[string]*ExecCtx, error) {

	ret := make(map[string]*ExecCtx)
	var i int
	var exec *Exec
	var ectx *ExecCtx
	var auth *Auth

	for i = 0; i < len(configuration.Exec); i++ {
		exec = &configuration.Exec[i]
		if ectx = ret[exec.Auth]; ectx == nil {

			if auth = configuration.Auth[exec.Auth]; auth == nil {
				return nil,
					fmt.Errorf(
						"Couldnt find auth entry: %s, as specified in exec at index %d",
						exec.Auth, i+1)
			}

			ret[exec.Auth] = &ExecCtx{}
			ret[exec.Auth].Auth = auth
			ret[exec.Auth].Exec = make([]*Exec, 1)
			ret[exec.Auth].Exec[0] = exec
			ret[exec.Auth].Name = exec.Auth
		} else {
			ret[exec.Auth].Exec = append(ret[exec.Auth].Exec, exec)
		}
	}

	return ret, nil
}
