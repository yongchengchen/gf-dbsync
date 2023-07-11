package mybinlog

type SimpleCanalDBConfig struct {
	Addr     string `toml:"address"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

// type CanalPipe struct {
// 	Pipe: map[string]string
// }
