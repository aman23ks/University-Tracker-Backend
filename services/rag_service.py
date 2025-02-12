from typing import Dict, List, Optional
import logging
from services.rag_retrieval import RAGRetrieval
from datetime import datetime
import json
import re
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class RAGService:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, cohere_api_key: str, index_name: str):
        self.retriever = RAGRetrieval(
            openai_api_key=openai_api_key,
            pinecone_api_key=pinecone_api_key,
            cohere_api_key=cohere_api_key,
            index_name=index_name
        )
        
    def format_answer(self, raw_answer: str) -> str:
        """Format and clean the RAG answer"""
        cleaned = raw_answer.split('[Chunk')[0].strip()
        cleaned = cleaned.split('Based on the context')[0].strip()
        
        # Remove common prefixes
        prefixes = [
            "According to the context, ",
            "Based on the provided information, ",
            "The context indicates that ",
            "From the available information, ",
            "The source states that "
        ]
        for prefix in prefixes:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
        
        return cleaned.strip()

    def _batch_process(self, texts: List[str], process_func) -> List:
        with ThreadPoolExecutor() as executor:
            return list(executor.map(process_func, texts))

    def process_university_data(self, scraped_data: List[Dict], university_name: str) -> Dict:
        try:
            logger.info(f"Processing data for {university_name}")
            
            # Prepare namespace
            namespace = f"urls_{university_name.lower().replace(' ', '_')}"
            
            # Extract and clean text content
            contents = []
            for data in scraped_data:
                if 'content' in data:
                    text = self._clean_text(data['content'])
                    if text:
                        contents.append({
                            'text': text,
                            'url': data['url'],
                            'metadata': data.get('metadata', {})
                        })

            # Process in batches
            batch_size = 5
            for i in range(0, len(contents), batch_size):
                batch = contents[i:i + batch_size]
                
                # Add to vector store
                success = self.retriever.index_documents(batch, namespace)
                if not success:
                    logger.error(f"Failed to index batch {i//batch_size + 1}")
                    continue
            
            return {
                'success': True,
                'processed_documents': len(contents),
                'namespace': namespace
            }
            
        except Exception as e:
            logger.error(f"Error processing university data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters
        text = re.sub(r'[^\w\s.,;?!-]', '', text)
        
        return text.strip()

    def query(self, question: str, namespace: str, metadata_filter: Optional[Dict] = None) -> Dict:
        try:
            logger.info(f"Processing query: {question}")
            
            # Get answer from RAG
            result = self.retriever.query(
                question=question,
                namespace=namespace,
                metadata_filter=metadata_filter
            )
            
            if not result['answer'] or result['answer'].startswith('Error'):
                return {
                    'answer': 'Information not available',
                    'confidence': 0,
                    'source_count': 0
                }
            
            # Format answer
            formatted_answer = self.format_answer(result['answer'])
            
            # Calculate confidence
            confidence = self._calculate_confidence(
                result['chunks_found'],
                formatted_answer,
                result.get('source_scores', [])
            )
            
            return {
                'answer': formatted_answer,
                'confidence': confidence,
                'source_count': result['chunks_found'],
                'sources': result.get('sources', [])
            }
            
        except Exception as e:
            logger.error(f"Error in RAG query: {str(e)}")
            return {
                'answer': 'Error processing query',
                'confidence': 0,
                'source_count': 0
            }

    def _calculate_confidence(self, chunks_found: int, answer: str, source_scores: List[float]) -> float:
        """Calculate confidence score based on multiple factors"""
        # Base confidence from number of chunks
        confidence = min(chunks_found / 5, 1.0)
        
        # Reduce confidence for uncertain answers
        uncertainty_phrases = ['maybe', 'might', 'could', 'possibly', 'unclear']
        if any(phrase in answer.lower() for phrase in uncertainty_phrases):
            confidence *= 0.8
            
        # Consider source scores if available
        if source_scores:
            avg_score = sum(source_scores) / len(source_scores)
            confidence = (confidence + avg_score) / 2
            
        return round(confidence, 2)

    def delete_university_data(self, university_name: str) -> bool:
        """Delete all vector data for a university"""
        try:
            namespace = f"urls_{university_name.lower().replace(' ', '_')}"
            return self.retriever.delete_namespace(namespace)
        except Exception as e:
            logger.error(f"Error deleting university data: {str(e)}")
            return False