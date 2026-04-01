export interface PhraseImprovement {
  original: string;
  improved: string;
  explanation: string;
  category: 'vocabulary' | 'grammar' | 'fluency' | 'idiom' | 'formality';
}

export interface SpeechFeedback {
  transcript: string;
  overallScore: number;
  improvements: PhraseImprovement[];
  strengths: string[];
  tips: string[];
}

export interface PitchPoint {
  time: number;
  frequency: number;
  volume: number;
}

export interface ShadowSegment {
  text: string;
  startTime: number;
  endTime: number;
}

export interface AnalysisResult {
  averagePitch: number;
  pitchRange: { min: number; max: number };
  wordsPerMinute: number;
  pauseCount: number;
  pitchData: PitchPoint[];
}

export type AppMode = 'practice' | 'shadow' | 'analyzer';

export interface Prompt {
  id: string;
  category: string;
  text: string;
  difficulty: 'beginner' | 'intermediate' | 'advanced';
}
