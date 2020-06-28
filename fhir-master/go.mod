module github.com/eug48/fhir

go 1.12

require (
	contrib.go.opencensus.io/exporter/jaeger v0.1.0
	contrib.go.opencensus.io/exporter/stackdriver v0.12.2
	github.com/DataDog/zstd v1.3.5
	github.com/bitly/go-simplejson v0.5.0
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/boj/redistore v0.0.0-20160128113310-fc113767cd6b // indirect
	github.com/buger/jsonparser v0.0.0-20180318095312-2cac668e8456
	github.com/campoy/embedmd v0.0.0-20181127030611-97c13d6e // indirect
	github.com/corpix/uarand v0.0.0-20170903190822-2b8494104d86 // indirect
	github.com/dlclark/regexp2 v1.1.6 // indirect
	github.com/dop251/goja v0.0.0-20180304123926-9183045acc25
	github.com/garyburd/redigo v1.6.0 // indirect
	github.com/gin-gonic/contrib v0.0.0-20180614032058-39cfb9727134
	github.com/gin-gonic/gin v0.0.0-20181126150151-b97ccf3a43d2
	github.com/go-sourcemap/sourcemap v2.1.2+incompatible // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/google/uuid v1.1.0
	github.com/gorilla/sessions v1.1.1 // indirect
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/itsjamie/gin-cors v0.0.0-20160420130702-97b4a9da7933
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/juju/errors v0.0.0-20170703010042-c7d06af17c68
	github.com/juju/loggo v0.0.0-20190526231331-6e530bcce5d8 // indirect
	github.com/juju/testing v0.0.0-20190613124551-e81189438503 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/mitre/heart v0.0.0-20160825192324-0c46b433a490
	github.com/opencensus-integrations/gomongowrapper v0.0.0-00010101000000-000000000000
	github.com/pebbe/util v0.0.0-20140716220158-e0e04dfe647c
	github.com/pkg/errors v0.8.1
	github.com/stretchr/objx v0.1.1 // indirect
	github.com/stretchr/testify v1.3.0
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/ugorji/go v1.1.5-pre // indirect
	go.mongodb.org/mongo-driver v1.1.4
	go.opencensus.io v0.22.0
	golang.org/x/crypto v0.0.0-20190621222207-cc06ce4a13d4 // indirect
	golang.org/x/lint v0.0.0-20190409202823-959b441ac422 // indirect
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859 // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sys v0.0.0-20200523222454-059865788121
	golang.org/x/tools v0.0.0-20190628021728-85b1a4bcd4e6 // indirect
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/genproto v0.0.0-20190627203621-eb59cef1c072 // indirect
	google.golang.org/grpc v1.21.1 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce
	gopkg.in/square/go-jose.v1 v1.1.1 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace github.com/opencensus-integrations/gomongowrapper => github.com/eug48/gomongowrapper v0.0.3

replace github.com/campoy/embedmd v0.0.0-20181127030611-97c13d6e => github.com/campoy/embedmd v0.0.0-20181127031020-97c13d6e4160

//replace github.com/opencensus-integrations/gomongowrapper => /home/user/src/gomongowrapper-eug48
