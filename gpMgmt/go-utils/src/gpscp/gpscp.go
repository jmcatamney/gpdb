package gpscp

func main() {
	defer DoTeardown()
	DoInit()
	DoFlagValidation()
	DoSetup()
	DoScp()
}
