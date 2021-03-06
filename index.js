const WebSocket = require('ws');
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
                            const {callId, sampleRate, fulfillParams, context, contextParams, from, to} = JSON.parse(data);
                            cId = callId;
                            this.logInfo({startCall: callId, from, to});
                            const session = this.sessionsClient.projectAgentSessionPath(this.config.projectId, callId);
                            return this.createDetectStream(callId, {
                                socket: ws,
                                sampleRate,
                                session,
                                fulfillParams: struct.encode(fulfillParams),
                                context,
                                contextParams: struct.encode(contextParams)
                            });
                        }
                        if (Buffer.isBuffer(data)) {
                            const connection = this.connections.get(cId);
                            if (connection?.dialogflow?.destroyed) return;
                            return connection?.dialogflow?.write({
                                session: connection?.session,
                                inputAudio: data
                            });
                        }
                        this.logInfo(data);
                    });
                    ws.on('close', () => {
                        this.logInfo({endCall: cId});
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

        async createDetectStream(callId, {socket, sampleRate, session, fulfillParams, context, contextParams}) {
            const dialogflow = this.sessionsClient.streamingDetectIntent();
            dialogflow
                .on('error', error => {
                    this.log.error(error);
                    if (socket.readyState === WebSocket.OPEN) {
                        socket.send(JSON.stringify({type: 'disconnect'}));
                        return socket.close();
                    }
                })
                .on('data', ({recognitionResult, queryResult}) => {
                    if (recognitionResult) {
                        if (recognitionResult.isFinal) {
                            this.connections.set(callId, {socket, session, sampleRate});
                            return dialogflow.end();
                        }
                    } else {
                        dialogflow.destroy();
                        const {fulfillmentText: text} = queryResult;
                        if (socket.readyState === WebSocket.OPEN) {
                            this.createDetectStream(callId, {socket, sampleRate, session, fulfillParams});
                            return this.synthesizeAndPlay({socket, text, sampleRate});
                        }
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
                        payload: fulfillParams
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
