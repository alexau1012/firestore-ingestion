package domain

type Meta struct {
	Type string `json:"type"`
	Ver  string `json:"ver"`
}

type Recommendations struct {
	Recommendations []string `json:"recommendations"`
	Meta            Meta     `json:"meta"`
}

type Config struct {
	UserIds         []string `json:"userIds"`
	Recommendations []string `json:"recommendations"`
	Meta            Meta     `json:"meta"`
}
