package nats

import (
	"fmt"
	"github.com/apcera/nats"
	"github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/pipelinemock"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"sync"
)

func NatsInputSpec(c gs.Context) {
	t := new(pipeline_ts.SimpleT)
	ctrl := gomock.NewController(t)

	pConfig := pipeline.NewPipelineConfig(nil)
	var wg sync.WaitGroup

	errChan := make(chan error, 1)
	retPackChan := make(chan *pipeline.PipelinePack, 1)
	defer close(errChan)
	defer close(retPackChan)

	c.Specify("A nats input", func() {
		input := new(NatsInput)
		ith := new(plugins_ts.InputTestHelper)
		ith.MockHelper = pipelinemock.NewMockPluginHelper(ctrl)
		ith.MockInputRunner = pipelinemock.NewMockInputRunner(ctrl)

		ith.Pack = pipeline.NewPipelinePack(pConfig.InputRecycleChan())
		ith.PackSupply = make(chan *pipeline.PipelinePack, 1)
		ith.PackSupply <- ith.Pack

		config := input.ConfigStruct().(*NatsInputConfig)
		config.Subject = "test"

		startInput := func() {
			wg.Add(1)
			go func() {
				errChan <- input.Run(ith.MockInputRunner, ith.MockHelper)
				wg.Done()
			}()
		}

		mockConn := new(mockConnection)
		mockConn.msgs = make(chan *nats.Msg, 1)
		defer close(mockConn.msgs)
		input.connectionProvider = func(opts *nats.Options) (Connection, error) {
			return mockConn, nil
		}

		inputName := "NatsInput"
		ith.MockInputRunner.EXPECT().Name().Return(inputName).AnyTimes()
		ith.MockInputRunner.EXPECT().InChan().Return(ith.PackSupply)
		ith.MockHelper.EXPECT().PipelineConfig().Return(pConfig)

		decoderName := "ScribbleDecoder"
		config.DecoderName = decoderName

		mockDecoderRunner := pipelinemock.NewMockDecoderRunner(ctrl)
		mockDecoderRunner.EXPECT().InChan().Return(retPackChan)
		ith.MockHelper.EXPECT().DecoderRunner(decoderName,
			fmt.Sprintf("%s-%s", inputName, decoderName),
		).Return(mockDecoderRunner, true)

		c.Specify("that is started", func() {
			err := input.Init(config)
			c.Expect(err, gs.IsNil)

			startInput()

			msg := &nats.Msg{
				Subject: config.Subject,
				Data:    []byte("test message"),
			}

			mockConn.msgs <- msg

			pack := <-retPackChan

			c.Expect(pack.Message.GetPayload(), gs.Equals, string(msg.Data))

			input.Stop()
			wg.Wait()
			c.Expect(<-errChan, gs.IsNil)
		})

	})

}
