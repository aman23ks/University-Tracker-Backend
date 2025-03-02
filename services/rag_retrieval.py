import asyncio
from typing import List, Dict
import logging
from openai import OpenAI
from pinecone import Pinecone
from datetime import datetime
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import cachetools

logger = logging.getLogger(__name__)

class RAGRetrieval:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, cohere_api_key: str, index_name: str):
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        self.embedding_cache = cachetools.TTLCache(maxsize=1000, ttl=3600)

    async def get_embedding_async(self, text: str) -> List[float]:
        """Get OpenAI embedding asynchronously"""
        try:
            response = await asyncio.to_thread(
                self.client.embeddings.create,
                model="text-embedding-ada-002",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"Error getting embedding: {str(e)}")
            return None

    async def search_pinecone(self, query: str, namespace: str, max_retries: int = 3, retry_delay: float = 2.0) -> List[Dict]:
        """Enhanced search with retries"""
        for attempt in range(max_retries):
            try:
                print(f"\n=== Search Debug (Attempt {attempt + 1}/{max_retries}) ===")
                print(f"Query: {query}")
                print(f"Namespace: {namespace}")

                # Get embedding
                query_embedding = await self.get_embedding_async(query)
                if not query_embedding:
                    print("❌ Failed to get query embedding")
                    return []
                print("✓ Got query embedding")

                # Get index stats
                stats = await asyncio.to_thread(self.index.describe_index_stats)
                print(f"\nIndex Stats:")
                print(f"Total vector count: {stats.total_vector_count}")
                print(f"Namespaces: {stats.namespaces}")
                print(f"Dimension: {stats.dimension}\n")

                if namespace not in stats.namespaces:
                    print(f"❌ Namespace {namespace} not found in index (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        print(f"Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    return []

                # Query Pinecone
                print(f"Querying Pinecone...")
                results = await asyncio.to_thread(
                    self.index.query,
                    vector=query_embedding,
                    namespace=namespace,
                    top_k=10,
                    include_metadata=True
                )
                
                print(f"\nQuery Results:")
                print(results)
                print(f"Number of matches: {len(results.matches)}")
                
                if not results.matches:
                    print("❌ No matches found in Pinecone")
                    if attempt < max_retries - 1:
                        print(f"Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    return []

                # Print all matches and scores before filtering
                print("\nAll matches before filtering:")
                for i, match in enumerate(results.matches):
                    print(f"\nMatch {i+1}:")
                    print(f"Score: {match.score}")
                    print(f"Metadata: {match.metadata}")

                processed_results = []
                for match in results.matches:
                    if match.score > 0.5:  # Lower threshold for testing
                        result = {
                            'text': match.metadata.get('text', ''),
                            'url': match.metadata.get('url', ''),
                            'similarity_score': match.score,
                            'metadata': match.metadata
                        }
                        processed_results.append(result)

                print(f"\nProcessed {len(processed_results)} results")
                return processed_results

            except Exception as e:
                print(f"❌ Error in search_pinecone (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    await asyncio.sleep(retry_delay)
                else:
                    return []

        return []

    async def query(self, question: str, namespace: str) -> Dict:
        """Process query and get response"""
        try:
            print(f"\nProcessing query: {question}")
            print(f"Using namespace: {namespace}")
            
            results = await self.search_pinecone(question, namespace)
            print(f"Found {len(results)} relevant documents")

            if not results:
                return {
                    "answer": "No relevant information found",
                    "sources": [],
                    "context": "",
                    "matches_found": 0
                }

            # Format context for better comprehension
            context_parts = []
            sources = []
            for r in results:
                score = r['similarity_score']
                if score > 0.7:
                    prefix = "Highly Relevant"
                elif score > 0.5:
                    prefix = "Relevant"
                else:
                    prefix = "Potentially Relevant"
                
                context_parts.append(
                    f"[{prefix} | Score: {score:.2f} | Source: {r['url']}]\n{r['text']}"
                )
                sources.append(r['url'])

            context = "\n\n".join(context_parts)
            print(f"Generated context with {len(context_parts)} parts")

            # Get answer from OpenAI
            print("Getting answer from OpenAI")
            answer = self.get_answer_from_openai(question, context)

            return {
                "answer": answer,
                "sources": sources[:3],  # Top 3 sources
                "context": "\n\n".join(context_parts[:2]),  # Top 2 contexts
                "matches_found": len(results)
            }

        except Exception as e:
            print(f"Error in query: {str(e)}")
            return {
                "answer": "Error processing query",
                "sources": [],
                "context": "",
                "error": str(e)
            }

    def get_answer_from_openai(self, question: str, context: str) -> str:
        """Get answer from OpenAI"""
        try:
            prompt = f"""
            Based on the following context, answer the question. Be precise and include relevant information from the context.
            If the context doesn't contain the answer, say "No relevant information found."

            Context:
            {context}

            Question: {question}

            Answer:"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content":  """You are an expert educational assistant specializing in university information. Your role is to help students find accurate details about university programs, admissions requirements, deadlines, courses, and other academic information.

                            When answering questions:
                            1. Focus on facts and specific information from the provided context
                            2. Present information in a clear, organized manner
                            3. When discussing requirements like GRE/TOEFL scores, tuition fees, or deadlines, be precise with numbers
                            4. For program-specific information, clearly indicate which program you're referring to
                            5. When the context contains multiple pieces of relevant information, structure your response logically
                            6. If the context doesn't contain the answer, honestly state "Based on the available information, I cannot find details about [topic]. You may want to check the university's official website or contact their admissions office."

                            Remember that students rely on your accuracy for important educational decisions. Always prioritize precision over comprehensiveness when uncertain."""
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=300
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"Error getting OpenAI response: {str(e)}")
            return "Error generating response"