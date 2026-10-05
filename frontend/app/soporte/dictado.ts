'use client';

/**
 * Dictado por voz, por dos caminos según lo que permita el navegador.
 *
 * **Chrome y Edge** traen reconocimiento de voz (`SpeechRecognition`). Es inmediato y no
 * hay nada que descargar, pero conviene saber lo que hace por dentro: Chrome no
 * transcribe en la máquina — manda el audio a los servidores de Google. Por eso Firefox
 * nunca lo implementó.
 *
 * **Firefox, Brave y los demás** no tienen nada de esto, y ninguna librería puede
 * añadirlo sin un modelo de reconocimiento. Ahí se ofrece Vosk: un modelo de español que
 * se descarga una vez (~40 MB) y corre dentro del navegador con WebAssembly. Más lento
 * de arrancar y algo menos preciso, pero **el audio no sale del equipo**.
 *
 * El modelo no se descarga solo: se pide al usuario, porque 40 MB a espaldas de alguien
 * no se hacen. Y si no está publicado en el servidor, se dice en vez de fallar callando.
 */
import { useEffect, useRef, useState } from 'react';

/** Dónde vive el modelo. Se sirve desde el propio servidor, no desde un CDN ajeno. */
const RUTA_MODELO = '/vosk/modelo-es.tar.gz';
/** A 16 kHz es como se entrenó el modelo; a otra frecuencia reconoce mucho peor. */
const FRECUENCIA = 16000;

export type MotorDictado = 'navegador' | 'vosk' | 'ninguno';

export interface Dictado {
  /** Qué puede usar este navegador. */
  motor: MotorDictado;
  grabando: boolean;
  /** Mientras se descarga y arranca el modelo de Vosk. */
  preparando: boolean;
  /** `null` si todo va bien; si no, qué pasó, en claro. */
  error: string | null;
  /** Vosk pide confirmación antes de descargar; con el navegador arranca directo. */
  alternar: () => void;
}

export function useDictado(alTexto: (fragmento: string) => void): Dictado {
  const [motor, setMotor] = useState<MotorDictado>('ninguno');
  const [grabando, setGrabando] = useState(false);
  const [preparando, setPreparando] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // El callback cambia en cada render; la referencia evita rearmar el reconocedor.
  const entregar = useRef(alTexto);
  entregar.current = alTexto;

  const nativo = useRef<any>(null);
  const vosk = useRef<{ reconocedor: any; modelo: any; audio: AudioContext; fuente: MediaStreamAudioSourceNode; nodo: ScriptProcessorNode } | null>(null);

  useEffect(() => {
    const Reconocimiento = (window as any).SpeechRecognition ?? (window as any).webkitSpeechRecognition;
    if (!Reconocimiento) {
      // Sin reconocimiento propio queda Vosk, que necesita micrófono y WebAssembly.
      // `getUserMedia` siempre existe como función en el tipo; lo que puede faltar es
      // `mediaDevices` entero, que es el caso en contextos no seguros.
      const hayMicro = typeof navigator.mediaDevices?.getUserMedia === 'function';
      setMotor(hayMicro && typeof WebAssembly === 'object' ? 'vosk' : 'ninguno');
      return;
    }

    const r = new Reconocimiento();
    r.lang = 'es-EC';
    r.continuous = true;
    r.interimResults = false;
    r.onresult = (e: any) => {
      for (let i = e.resultIndex; i < e.results.length; i++) {
        if (e.results[i].isFinal) entregar.current(e.results[i][0].transcript.trim());
      }
    };
    r.onend = () => setGrabando(false);
    r.onerror = (e: any) => {
      setGrabando(false);
      if (e?.error === 'not-allowed') setError('No se dio permiso al micrófono.');
    };

    nativo.current = r;
    setMotor('navegador');
    return () => {
      try {
        r.stop();
      } catch {
        /* ya estaba parado */
      }
    };
  }, []);

  /** Suelta el micrófono y el procesador: si no, el indicador de grabación se queda puesto. */
  const soltarVosk = () => {
    const v = vosk.current;
    if (!v) return;
    try {
      v.nodo.disconnect();
      v.fuente.disconnect();
      v.fuente.mediaStream.getTracks().forEach((t) => t.stop());
      void v.audio.close();
      v.reconocedor.remove();
      v.modelo.terminate();
    } catch {
      /* cerrando algo ya cerrado */
    }
    vosk.current = null;
  };

  useEffect(() => () => soltarVosk(), []);

  const arrancarVosk = async () => {
    setError(null);
    setPreparando(true);
    try {
      // Antes de bajar 40 MB se comprueba que el modelo esté publicado: si falta, es un
      // despliegue incompleto y es mejor decirlo que dejar una barra girando.
      const sonda = await fetch(RUTA_MODELO, { method: 'HEAD' });
      if (!sonda.ok) throw new Error('falta-modelo');

      const vb = await import('vosk-browser');
      const modelo = await vb.createModel(RUTA_MODELO);
      const reconocedor = new modelo.KaldiRecognizer(FRECUENCIA);
      reconocedor.on('result', (m: any) => {
        const t = m?.result?.text?.trim();
        if (t) entregar.current(t);
      });

      const micro = await navigator.mediaDevices.getUserMedia({
        audio: { channelCount: 1, sampleRate: FRECUENCIA, echoCancellation: true, noiseSuppression: true },
      });
      const audio = new AudioContext();
      const fuente = audio.createMediaStreamSource(micro);
      const nodo = audio.createScriptProcessor(4096, 1, 1);
      nodo.onaudioprocess = (e) => {
        try {
          reconocedor.acceptWaveform(e.inputBuffer);
        } catch {
          /* un trozo de audio perdido no interrumpe el dictado */
        }
      };
      fuente.connect(nodo);
      nodo.connect(audio.destination);

      vosk.current = { reconocedor, modelo, audio, fuente, nodo };
      setGrabando(true);
    } catch (e) {
      const causa = (e as Error)?.message;
      setError(
        causa === 'falta-modelo'
          ? 'El modelo de voz no está instalado en el servidor. Avise a soporte.'
          : causa === 'Permission denied' || (e as any)?.name === 'NotAllowedError'
            ? 'No se dio permiso al micrófono.'
            : 'No se pudo preparar el dictado en este navegador.',
      );
      soltarVosk();
    } finally {
      setPreparando(false);
    }
  };

  const alternar = () => {
    setError(null);

    if (motor === 'navegador') {
      const r = nativo.current;
      if (!r) return;
      try {
        if (grabando) r.stop();
        else {
          r.start();
          setGrabando(true);
        }
      } catch {
        setGrabando(false);
      }
      return;
    }

    if (motor === 'vosk') {
      if (grabando) {
        soltarVosk();
        setGrabando(false);
      } else if (!preparando) {
        void arrancarVosk();
      }
    }
  };

  return { motor, grabando, preparando, error, alternar };
}
