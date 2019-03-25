package basic

type managerConfig struct {
	Users []*userConfig
	Roles []*roleConfig
}

type userConfig struct {
	Role     string
	Username string
	Password string
}

type roleConfig struct {
	Role   string
	Accept []string
	Reject []string
}
