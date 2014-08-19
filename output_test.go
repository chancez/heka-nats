package nats

import (
	"github.com/apcera/nats"
	"github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/plugins"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"sync"
)

func NatsOutputSpec(c gs.Context) {
	t := new(pipeline_ts.SimpleT)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pConfig := pipeline.NewPipelineConfig(nil)
	var wg sync.WaitGroup

	errChan := make(chan error, 1)
	defer close(errChan)

	c.Specify("A nats output", func() {
		output := new(NatsOutput)
		oth := plugins_ts.NewOutputTestHelper(ctrl)
		config := output.ConfigStruct().(*NatsOutputConfig)
		config.Subject = "test"

		startOutput := func() {
			wg.Add(1)
			go func() {
				errChan <- output.Run(oth.MockOutputRunner, oth.MockHelper)
				wg.Done()
			}()
		}

		mockConn := new(mockConnection)
		mockConn.msgs = make(chan *nats.Msg, 1)
		defer close(mockConn.msgs)
		output.newConnection = func(opts *nats.Options) (Connection, error) {
			return mockConn, nil
		}

		msg := pipeline_ts.GetTestMessage()
		pack := pipeline.NewPipelinePack(pConfig.InputRecycleChan())
		pack.Message = msg

		c.Specify("requires an encoder", func() {
			err := output.Init(config)
			c.Assume(err, gs.IsNil)

			oth.MockOutputRunner.EXPECT().Encoder().Return(nil)
			err = output.Run(oth.MockOutputRunner, oth.MockHelper)
			c.Expect(err, gs.Not(gs.IsNil))
		})

		c.Specify("that is started", func() {
			encoder := new(plugins.PayloadEncoder)
			enConf := new(plugins.PayloadEncoderConfig)
			enConf.PrefixTs = false
			encoder.Init(enConf)
			oth.MockOutputRunner.EXPECT().Encoder().Return(encoder)
			oth.MockOutputRunner.EXPECT().Encode(pack).Return(encoder.Encode(pack)).AnyTimes()

			inChan := make(chan *pipeline.PipelinePack, 1)
			oth.MockOutputRunner.EXPECT().InChan().Return(inChan)

			c.Specify("publishes messages with the configured subject", func() {
				err := output.Init(config)
				c.Assume(err, gs.IsNil)

				startOutput()

				inChan <- pack

				m := <-mockConn.msgs
				c.Expect(m.Subject, gs.Equals, config.Subject)
				c.Expect(string(m.Data), gs.Equals, msg.GetPayload())

				close(inChan)
				wg.Wait()
				c.Expect(<-errChan, gs.IsNil)
			})
		})

	})

}
