const {struct} = require('pb-util');
const {SessionsClient} = require('@google-cloud/dialogflow');
const {TextToSpeechClient} = require('@google-cloud/text-to-speech');

module.exports = function listen() {
    return class listen extends require('ut-port-wss')(...arguments) {
        get defaults() {
            return {
                namespace: ['listen'],
                server: {
                    port: 8080
                },
                rooms: ['calls']
            };
        }

        async init() {
            this.connections = new Map();
            this.speechClient = new TextToSpeechClient();
            this.sessionsClient = new SessionsClient();

            await super.init(...arguments);

            Object.entries(this.socketServers).forEach(([name, wss]) => {
                wss.on('connection', ws => {
                    let callId,
                        sampleRate,
                        context;
                    let initial = true;

                    ws.on('message', data => {
                        if (initial) {
                            initial = false;
                            ({callId, sampleRate, context} = JSON.parse(data));
                            const session = this.sessionsClient.projectAgentSessionPath(this.config.projectId, callId);
                            this.playWelcome({socket: ws, session, sampleRate});
                            return this.createDetectStream(callId, {
                                socket: ws,
                                sampleRate,
                                session,
                                context: struct.encode(context)
                            });
                        }
                        if (Buffer.isBuffer(data)) {
                            const connection = this.connections.get(callId);
                            return connection?.dialogflow?.write({
                                session: connection?.session,
                                inputAudio: data
                            });
                        }
                        this.logInfo(data);
                    });
                    ws.on('close', () => {
                        ws.closed = true;
                        const connection = this.connections.get(callId);
                        connection?.dialogflow?.end();
                        connection?.dialogflow?.destroy();
                        this.connections.delete(callId);
                    });
                });
            });
        }

        async playWelcome({socket, session, sampleRate}) {
            const [{queryResult: {fulfillmentText: text}}] = await this.sessionsClient
                .detectIntent({session, queryInput: {event: { name: 'Welcome', parameters: {}, languageCode: 'bg-bg'}}});
            return this.synthesizeAndPlay({socket, text, sampleRate});
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

        async createDetectStream(callId, {socket, sampleRate, session, context}) {
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
                        !socket.closed && this.createDetectStream(callId, {socket, sampleRate, session, context});
                        return this.synthesizeAndPlay({socket, text, sampleRate});
                    }
                })
                .write({
                    session,
                    queryInput: {
                        audioConfig: {
                            audioEncoding: 'AUDIO_ENCODING_LINEAR_16',
                            sampleRateHertz: sampleRate,
                            languageCode: 'bg-bg'
                        },
                        singleUtterance: true
                    },
                    queryParams: {
                        payload: context
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
