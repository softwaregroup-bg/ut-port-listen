const {struct} = require('pb-util');
const {SessionsClient} = require('@google-cloud/dialogflow');
const {TextToSpeechClient} = require('@google-cloud/text-to-speech');

module.exports = function listen() {
    return class listen extends require('ut-port-wss')(...arguments) {
        get defaults() {
            return {
                namespace: ['listen'],
                log: {
                    transform: {
                        privateKey: 'mask'
                    }
                },
                server: {
                    port: 8087
                },
                rooms: ['calls']
            };
        }

        get schema() {
            return {
                type: 'object',
                properties: {
                    email: {
                        type: 'string'
                    },
                    privateKey: {
                        type: 'string'
                    },
                    projectId: {
                        type: 'string'
                    }
                },
                required: ['projectId']
            };
        }

        async init() {
            this.connections = new Map();
            const credentials = (
                this.config.email && this.config.privateKey && {
                    client_email: this.config.email,
                    private_key: this.config.privateKey
                }
            ) || undefined;
            this.speechClient = new TextToSpeechClient({
                credentials,
                projectId: this.config.projectId
            });
            this.sessionsClient = new SessionsClient({
                credentials,
                projectId: this.config.projectId
            });

            await super.init(...arguments);

            Object.entries(this.socketServers).forEach(([name, wss]) => {
                wss.on('connection', ws => {
                    let initial = true;
                    let cId;

                    ws.on('message', data => {
                        if (initial) {
                            initial = false;
                            const {callId, sampleRate, fulfilParams, context, contextParams} = JSON.parse(data);
                            cId = callId;
                            const session = this.sessionsClient.projectAgentSessionPath(this.config.projectId, callId);
                            return this.createDetectStream(callId, {
                                socket: ws,
                                sampleRate,
                                session,
                                fulfilParams: struct.encode(fulfilParams),
                                context,
                                contextParams: struct.encode(contextParams)
                            });
                        }
                        if (Buffer.isBuffer(data)) {
                            const connection = this.connections.get(cId);
                            return connection?.dialogflow?.write({
                                session: connection?.session,
                                inputAudio: data
                            });
                        }
                        this.logInfo(data);
                    });
                    ws.on('close', () => {
                        ws.closed = true;
                        const connection = this.connections.get(cId);
                        connection?.dialogflow?.end();
                        connection?.dialogflow?.destroy();
                        this.connections.delete(cId);
                    });
                });
            });
        }

        async synthesizeAndPlay({socket, text, sampleRate}) {
            const [{audioContent}] = await this.speechClient.synthesizeSpeech({
                input: {text},
                voice: {languageCode: 'bg-BG', ssmlGender: 'NEUTRAL'},
                audioConfig: {sampleRateHertz: sampleRate, audioEncoding: 'LINEAR16'}
            });
            socket.send(JSON.stringify({
                type: 'playAudio',
                data: {
                    sampleRate,
                    audioContentType: 'raw',
                    audioContent: Buffer.from(audioContent).toString('base64')
                }
            }));
        }

        async createDetectStream(callId, {socket, sampleRate, session, fulfilParams, context, contextParams}) {
            const dialogflow = this.sessionsClient.streamingDetectIntent();
            dialogflow
                .on('error', this.log.error)
                .on('data', ({recognitionResult, queryResult}) => {
                    if (recognitionResult) {
                        this.logInfo({Transcript: recognitionResult.transcript});
                        if (recognitionResult.isFinal) {
                            this.connections.set(callId, {socket, session, sampleRate});
                            return dialogflow.end();
                        }
                    } else {
                        dialogflow.destroy();
                        const {fulfillmentText: text} = queryResult;
                        this.logInfo({Fulfillment: text});
                        !socket.closed && this.createDetectStream(callId, {socket, sampleRate, session, fulfilParams});
                        return this.synthesizeAndPlay({socket, text, sampleRate});
                    }
                })
                .write({
                    session,
                    queryInput: {
                        audioConfig: {
                            audioEncoding: 'AUDIO_ENCODING_LINEAR_16',
                            sampleRateHertz: sampleRate,
                            languageCode: 'bg-bg',
                            singleUtterance: true
                        },
                        ...context && {
                            event: {
                                name: context,
                                parameters: contextParams,
                                languageCode: 'bg-bg'
                            }
                        }
                    },
                    queryParams: {
                        payload: fulfilParams
                    }
                });
            this.connections.set(callId, {socket, dialogflow, session, sampleRate});
        }

        logInfo(data) {
            this.log.info && this.log.info(JSON.stringify(data));
        }

        handlers() {
            const send = (callId, params) => this.connections.get(callId)?.socket?.send(JSON.stringify(params));
            return {
                'listen.call.playAudio'({
                    callId,
                    audioContent,
                    audioContentType,
                    sampleRate
                }) {
                    send(callId, {
                        type: 'playAudio',
                        data: {
                            sampleRate,
                            audioContentType,
                            audioContent: Buffer.from(audioContent).toString('base64')
                        }
                    });
                    return {callId};
                },
                'listen.call.killAudio'({callId}) {
                    send(callId, {type: 'killAudio'});
                    return {callId};
                },
                'listen.call.disconnect'({callId}) {
                    send(callId, {type: 'disconnect'});
                    return {callId};
                },
                'listen.call.transfer'({callId}) {
                    send(callId, {type: 'transfer', data: {}}); // TBD
                },
                'listen.call.transcription'({callId}) {
                    send(callId, {type: 'transcription', data: {}}); // TBD
                }
            };
        }
    };
};
