/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Chance Zibolski (chance.zibolski@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package nats

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/apcera/nats"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"time"
)

type NatsInputConfig struct {
	Url            string   `toml:"url"`
	Servers        []string `toml:"servers"`
	NoRandomize    bool     `toml:"no_randomize"`
	Subject        string   `toml:"subject"`
	AllowReconnect bool     `toml:"reconnect"`
	MaxReconnect   int      `toml:"max_reconnects"`
	// Time in Seconds to wait before reconnecting
	ReconnectWait uint32 `toml:"reconnect_wait"`
	// Timeout in Milliseconds
	Timeout     uint32 `toml:"timeout"`
	DecoderName string `toml:"decoder"`
	UseMsgBytes *bool  `toml:"use_msgbytes"`
}

type NatsInput struct {
	*NatsInputConfig
	Conn          Connection
	Options       *nats.Options
	decoderChan   chan *pipeline.PipelinePack
	runner        pipeline.InputRunner
	stop          chan struct{}
	newConnection func(*nats.Options) (Connection, error)
	pConfig       *pipeline.PipelineConfig
}

func (input *NatsInput) ConfigStruct() interface{} {
	return &NatsInputConfig{
		Url: nats.DefaultURL,
	}
}

func (input *NatsInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	input.pConfig = pConfig
}

func (input *NatsInput) Init(config interface{}) error {
	conf := config.(*NatsInputConfig)
	input.NatsInputConfig = conf
	input.stop = make(chan struct{}, 1)
	options := &nats.Options{
		Url:            conf.Url,
		Servers:        conf.Servers,
		NoRandomize:    conf.NoRandomize,
		AllowReconnect: conf.AllowReconnect,
		MaxReconnect:   conf.MaxReconnect,
	}

	if conf.ReconnectWait > 0 {
		options.ReconnectWait = time.Duration(conf.ReconnectWait) * time.Second
	}
	if conf.Timeout > 0 {
		options.Timeout = time.Duration(conf.Timeout) * time.Millisecond
	}
	options.ClosedCB = func(c *nats.Conn) {
		input.Stop()
	}
	input.Options = options
	if input.newConnection == nil {
		input.newConnection = defaultConnectionProvider
	}

	var (
		useMsgBytes bool
		ok          bool
		decoder     pipeline.Decoder
	)

	if input.UseMsgBytes == nil {
		// Only override if not already set
		if conf.DecoderName != "" {
			decoder, ok = input.pConfig.Decoder(conf.DecoderName)
		}
		if ok && decoder != nil {
			// We want to know what kind of decoder is being used, but we only
			// care if they're using a protobuf decoder, or a multidecoder with
			// a protobuf decoder as the first sub decoder
			switch decoder.(type) {
			case *pipeline.ProtobufDecoder:
				useMsgBytes = true
			case *pipeline.MultiDecoder:
				d := decoder.(*pipeline.MultiDecoder)
				if len(d.Decoders) > 0 {
					if _, ok := d.Decoders[0].(*pipeline.ProtobufDecoder); ok {
						useMsgBytes = true
					}
				}
			}
		}
		input.UseMsgBytes = &useMsgBytes
	}

	return nil
}

func (input *NatsInput) Run(runner pipeline.InputRunner,
	helper pipeline.PluginHelper) (err error) {

	var (
		dRunner pipeline.DecoderRunner
		pack    *pipeline.PipelinePack
		ok      bool
	)

	if input.DecoderName != "" {
		if dRunner, ok = input.pConfig.DecoderRunner(input.DecoderName,
			fmt.Sprintf("%s-%s", runner.Name(), input.DecoderName)); !ok {
			return fmt.Errorf("Decoder not found: %s", input.DecoderName)
		}
		input.decoderChan = dRunner.InChan()
	}
	input.runner = runner

	hostname := input.pConfig.Hostname()
	packSupply := runner.InChan()

	input.Conn, err = input.newConnection(input.Options)

	if err != nil {
		return
	}
	defer close(input.stop)
	defer input.Conn.Close()

	input.Conn.Subscribe(input.Subject, func(msg *nats.Msg) {
		pack = <-packSupply
		// If we're using protobuf, then the entire message is in the
		// nats message body
		if *input.UseMsgBytes {
			pack.MsgBytes = msg.Data
		} else {
			pack.Message.SetUuid(uuid.NewRandom())
			pack.Message.SetTimestamp(time.Now().UnixNano())
			pack.Message.SetType("nats.input")
			pack.Message.SetHostname(hostname)
			pack.Message.SetPayload(string(msg.Data))
			message.NewStringField(pack.Message, "subject", msg.Subject)
		}
		input.sendPack(pack)
	})

	// Wait for the channel to tell us to stop
	<-input.stop
	return nil
}

func (input *NatsInput) Stop() {
	if input.stop != nil {
		input.stop <- struct{}{}
	}
}

func (input *NatsInput) sendPack(pack *pipeline.PipelinePack) {
	if input.decoderChan != nil {
		input.decoderChan <- pack
	} else {
		input.runner.Inject(pack)
	}
}

func init() {
	pipeline.RegisterPlugin("NatsInput", func() interface{} {
		return new(NatsInput)
	})
}

func defaultConnectionProvider(opts *nats.Options) (Connection, error) {
	return opts.Connect()
}
