from typing import List, Dict
import logging
from openai import OpenAI
import cohere
from pinecone import Pinecone
from datetime import datetime

logger = logging.getLogger(__name__)

class RAGRetrieval:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, cohere_api_key: str, index_name: str):
        self.client = OpenAI(api_key=openai_api_key)
        self.co = cohere.Client(cohere_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)

        self.system_prompt = """You are a highly precise question-answering system. Answer based ONLY on the provided context. Rules:
1. Only use information from the context
2. If context doesn't have enough information, say "I cannot answer based on the provided context"
3. Include relevant quotes to support your answer
4. Be concise and precise

Context:
{context}

Question: {question}

Answer: """

    def get_embedding(self, text: str) -> List[float]:
        """Get OpenAI embedding"""
        try:
            response = self.client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            return []

    # Update this method in rag_retrieval.py
    def rerank_with_cohere(self, query: str, docs: List[str], top_k: int = 20) -> List[Dict]:
        """Rerank documents using Cohere"""
        try:
            rerank_response = self.co.rerank(
                query=query,
                documents=docs,
                top_n=top_k,
                model='rerank-english-v2.0'
            )

            # Process reranked results from the results field
            processed_results = []
            for reranked_item in rerank_response.results:
                processed_results.append({
                    'index': reranked_item.index,
                    'relevance_score': float(reranked_item.relevance_score),
                    'document': docs[reranked_item.index]
                })

            # Sort by relevance score in descending order
            processed_results.sort(key=lambda x: x['relevance_score'], reverse=True)
            return processed_results[:top_k]

        except Exception as e:
            logger.error(f"Error in reranking: {str(e)}")
            return []
        
    def search_pinecone(self, query: str, namespace: str, top_k: int = 20) -> List[Dict]:
        """Search Pinecone with embedded query"""
        try:
            query_embedding = self.get_embedding(query)
            if not query_embedding:
                return []

            # Query Pinecone
            results = self.index.query(
                vector=query_embedding,
                namespace=namespace,
                top_k=top_k,
                include_metadata=True
            )

            if not results.matches:
                return []

            # Process Pinecone results
            processed_docs = []
            for match in results.matches:
                doc = {
                    'text': match.metadata.get('text', ''),
                    'url': match.metadata.get('url', ''),
                    'vector_score': float(match.score),
                    'metadata': match.metadata
                }
                processed_docs.append(doc)

            # Get texts for reranking
            texts = [doc['text'] for doc in processed_docs]
            
            # Rerank documents
            reranked_results = self.rerank_with_cohere(query, texts)

            # Combine original docs with reranking scores
            final_results = []
            for reranked in reranked_results:
                original_doc = processed_docs[reranked['index']]
                final_results.append({
                    **original_doc,
                    'rerank_score': reranked['relevance_score']
                })

            # Sort by rerank score
            final_results.sort(key=lambda x: x['rerank_score'], reverse=True)
            return final_results

        except Exception as e:
            logger.error(f"Error searching Pinecone: {str(e)}")
            return []

    def query(self, question: str, namespace: str) -> Dict:
        """Main query method"""
        try:
            # Get top reranked results
            results = self.search_pinecone(question, namespace)
            
            if not results:
                return {
                    "answer": "No relevant information found",
                    "sources": [],
                    "context": "",
                    "matches_found": 0
                }

            # Format context from top results
            context_parts = []
            for r in results[:20]:  # Use top 20 reranked results
                context_parts.append(
                    f"[Source: {r['url']}]\n"
                    f"{r['text']}\n"
                    f"Relevance Score: {r['rerank_score']:.4f}"
                )

            context = "\n\n".join(context_parts)

            # Get answer from OpenAI
            answer = self.get_answer_from_openai(question, context)

            return {
                "answer": answer,
                "sources": [r['url'] for r in results[:5]],  # Top 5 sources
                "context": context,
                "matches_found": len(results)
            }

        except Exception as e:
            logger.error(f"Error in query pipeline: {str(e)}")
            return {
                "answer": "Error processing query",
                "sources": [],
                "context": "",
                "error": str(e)
            }

    def get_answer_from_openai(self, query: str, context: str) -> str:
        """Get answer from OpenAI using context"""
        try:
            prompt = self.system_prompt.format(
                context=context,
                question=query
            )

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=250
            )

            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Error getting OpenAI response: {str(e)}")
            return "Error generating response"
