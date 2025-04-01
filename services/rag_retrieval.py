import asyncio
from typing import List, Dict, Tuple, Any
import logging
from openai import OpenAI
from pinecone import Pinecone
from datetime import datetime
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import cachetools
import re
import hashlib

logger = logging.getLogger(__name__)

class RAGRetrieval:
    def __init__(self, openai_api_key: str, pinecone_api_key: str, index_name: str):
        self.client = OpenAI(api_key=openai_api_key)
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index = self.pc.Index(index_name)
        self.embedding_cache = cachetools.TTLCache(maxsize=1000, ttl=3600)

    async def get_embedding_async(self, text: str) -> List[float]:
        """Get OpenAI embedding asynchronously with caching"""
        # Check cache first
        cache_key = hashlib.md5(text.encode()).hexdigest()
        if cache_key in self.embedding_cache:
            return self.embedding_cache[cache_key]
        
        try:
            # Truncate text if too long for OpenAI API
            if len(text) > 8000:
                text = text[:8000]
                
            response = await asyncio.to_thread(
                self.client.embeddings.create,
                model="text-embedding-ada-002",
                input=text
            )
            embedding = response.data[0].embedding
            
            # Cache the result
            self.embedding_cache[cache_key] = embedding
            return embedding
        except Exception as e:
            print(f"Error getting embedding: {str(e)}")
            return None

    async def search_pinecone(self, query: str, namespace: str, max_retries: int = 3, retry_delay: float = 2.0) -> List[Dict]:
        """Enhanced search with retries and better debugging"""
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
                print(f"Namespaces: {list(stats.namespaces.keys()) if stats.namespaces else 'None'}")
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
                    top_k=20,  # Increased from 10 to 20 for more candidates
                    include_metadata=True
                )
                
                print(f"\nQuery Results:")
                print(f"Number of matches: {len(results.matches)}")
                
                if not results.matches:
                    print("❌ No matches found in Pinecone")
                    if attempt < max_retries - 1:
                        print(f"Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                        continue
                    return []

                # Advanced filtering and scoring with cosine_similarity
                # First get the original matches
                matches = results.matches
                
                # Print all matches and scores before filtering
                print("\nAll matches before filtering:")
                for i, match in enumerate(matches):
                    print(f"\nMatch {i+1}:")
                    print(f"Score: {match.score}")
                    print(f"URL: {match.metadata.get('url', 'No URL')}")
                    print(f"Title: {match.metadata.get('title', 'No title')}")

                # Process results with metadata
                processed_results = []
                
                # Sort by score (usually already sorted, but just to be safe)
                matches.sort(key=lambda x: x.score, reverse=True)
                
                for match in matches:
                    # Apply a threshold filter - but keep it low to include more candidates
                    if match.score > 0.4:  # Lowered threshold
                        result = {
                            'text': match.metadata.get('text', ''),
                            'url': match.metadata.get('url', ''),
                            'title': match.metadata.get('title', ''),
                            'program': match.metadata.get('program', ''),
                            'original_url': match.metadata.get('original_url', match.metadata.get('url', '')),
                            'similarity_score': match.score,
                            'metadata': match.metadata
                        }
                        processed_results.append(result)

                print(f"\nProcessed {len(processed_results)} results after filtering")
                return processed_results

            except Exception as e:
                print(f"❌ Error in search_pinecone (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    await asyncio.sleep(retry_delay)
                else:
                    return []

        return []

    def rerank_results(self, query: str, results: List[Dict]) -> List[Dict]:
        """
        Rerank results using cosine similarity between query and text chunks
        """
        if not results:
            return []
            
        try:
            # Get query embedding
            query_embedding = self.get_embedding_sync(query)
            if not query_embedding:
                print("⚠️ Could not get query embedding for reranking")
                return results  # Return original results if embedding fails
                
            # Get embeddings for each text chunk
            text_embeddings = []
            for result in results:
                text = result['text']
                embedding = self.get_embedding_sync(text)
                if embedding:
                    text_embeddings.append(embedding)
                else:
                    text_embeddings.append([0] * len(query_embedding))  # Fallback
            
            # Convert to numpy arrays
            query_embedding_np = np.array(query_embedding).reshape(1, -1)
            text_embeddings_np = np.array(text_embeddings)
            
            # Calculate cosine similarity
            similarity_scores = cosine_similarity(query_embedding_np, text_embeddings_np)[0]
            
            # Add scores to results
            for i, score in enumerate(similarity_scores):
                results[i]['cosine_score'] = float(score)
                # Create a combined score (blend of original and cosine)
                results[i]['combined_score'] = (results[i]['similarity_score'] + float(score)) / 2
            
            # Sort by combined score
            results.sort(key=lambda x: x['combined_score'], reverse=True)
            
            print("✓ Reranked results with cosine similarity")
            return results
            
        except Exception as e:
            print(f"⚠️ Error during reranking: {str(e)}")
            return results  # Return original results if reranking fails
    
    def get_embedding_sync(self, text: str) -> List[float]:
        """Synchronous version of get_embedding"""
        try:
            # Check cache first
            cache_key = hashlib.md5(text.encode()).hexdigest()
            if cache_key in self.embedding_cache:
                return self.embedding_cache[cache_key]
            
            # Truncate text if too long
            if len(text) > 8000:
                text = text[:8000]
                
            response = self.client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            embedding = response.data[0].embedding
            
            # Cache the result
            self.embedding_cache[cache_key] = embedding
            return embedding
        except Exception as e:
            print(f"Error getting embedding: {str(e)}")
            return None

    async def query(self, question: str, namespace: str) -> Dict:
        """Process query and get response with proper source tracking"""
        try:
            print(f"\nProcessing query: {question}")
            print(f"Using namespace: {namespace}")
            
            # Get search results from Pinecone
            results = await self.search_pinecone(question, namespace)
            print(f"Found {len(results)} relevant documents from Pinecone")

            if not results:
                return {
                    "answer": "I don't have enough information to answer this question based on the available data. Please try a different question or check the university's official website.",
                    "sources": [],
                    "context": "",
                    "matches_found": 0
                }

            # Rerank results with cosine similarity
            results = self.rerank_results(question, results)
            
            # Format context for better comprehension
            context_parts = []
            source_info = []
            
            for i, r in enumerate(results[:10]):  # Consider top 10 for context
                # Create a source ID to track which source was used
                source_id = f"[SOURCE-{i+1}]"
                
                # Add source information
                source_info.append({
                    "id": source_id,
                    "url": r['url'],
                    "title": r.get('title', ''),
                    "program": r.get('program', ''),
                    "similarity_score": r['similarity_score'],
                    "cosine_score": r.get('cosine_score', 0),
                    "combined_score": r.get('combined_score', r['similarity_score']),
                })
                
                # Format the context with source ID
                context_parts.append(
                    f"{source_id}\n{r['text']}\n"
                )

            context = "\n\n".join(context_parts)
            print(f"Generated context with {len(context_parts)} parts")

            # Get answer from OpenAI with source citation instructions
            print("Getting answer from OpenAI with source citation instructions")
            answer, used_sources = self.get_answer_from_openai_with_sources(question, context, source_info)
            
            # Format the final response with proper line breaks and sources
            formatted_answer = self.format_response_with_sources(answer, used_sources)

            # Build final response
            response = {
                "answer": formatted_answer,  # Use the formatted answer with sources
                "raw_answer": answer,        # Keep the original answer for reference
                "sources": used_sources,     # The sources that were actually used
                "all_sources": source_info[:5],  # Top 5 sources
                "matches_found": len(results)
            }
            
            return response

        except Exception as e:
            print(f"Error in query: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "answer": "Error processing query",
                "sources": [],
                "context": "",
                "error": str(e)
            }

    def get_answer_from_openai_with_sources(self, question: str, context: str, source_info: List[Dict]) -> Tuple[str, List[Dict]]:
        """
        Get answer from OpenAI with explicit instructions to cite sources using the source IDs
        
        Returns:
            Tuple of (answer_text, list_of_used_sources)
        """
        try:
            # Create a prompt that instructs the model to cite sources
            prompt = f"""
            Based on the following context, answer the question. Be specific and include relevant information from the context.
            Use proper formatting with line breaks for readability.
            
            IMPORTANT: When you use information from a specific source, cite it using its ID (like [SOURCE-1], [SOURCE-2], etc.) inline after the relevant information.
            If the context doesn't contain the answer, say "Based on the available information, I cannot answer this question."

            Context:
            {context}

            Question: {question}

            Answer (with source citations):"""

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": """You are an expert educational assistant specializing in university information. Your role is to help students find accurate details about university programs, admissions requirements, deadlines, courses, and other academic information.

                        When answering questions:
                        1. Focus on facts and specific information from the provided context
                        2. Present information in a clear, organized manner with proper line breaks and formatting
                        3. IMPORTANT: Cite your sources using the source IDs provided in the context ([SOURCE-1], [SOURCE-2], etc.) in your answer
                        4. When multiple sources provide similar information, cite all relevant sources
                        5. When discussing requirements like GRE/TOEFL scores, tuition fees, or deadlines, be precise with numbers
                        6. If the context doesn't contain the answer, honestly state that you cannot find the information
                        7. Remember to reply for only Masters of Science (MS) in Computer Science (CS/CSE) program and do not include information related to undegrad and phd. 
                        8. Try to answer based on the context provided to you with sources if you cannot find the answer in the documents then use the web to find the most recent and relevant response and answer the question.
                        
                        For example, instead of just saying "The program requires a GRE score of 300." say "The program requires a GRE score of 300 [SOURCE-2]."
                        """
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=4000
            )

            answer_text = response.choices[0].message.content.strip()
            
            # Extract source IDs mentioned in the answer using regex
            source_pattern = r'\[SOURCE-(\d+)\]'
            source_matches = re.findall(source_pattern, answer_text)
            
            # Get unique source IDs as integers
            unique_source_ids = sorted(set([int(s) for s in source_matches]))
            
            # Get the corresponding source info
            used_sources = []
            for source_id in unique_source_ids:
                # Source IDs are 1-indexed in the text but 0-indexed in our list
                if 0 <= source_id-1 < len(source_info):
                    used_sources.append(source_info[source_id-1])
            
            print(f"Answer uses {len(used_sources)} sources: {', '.join([f'SOURCE-{s}' for s in unique_source_ids])}")
            print(f"Used sources details: {used_sources}")
            
            return answer_text, used_sources

        except Exception as e:
            print(f"Error getting OpenAI response: {str(e)}")
            return "Error generating response", []
    
    def format_response_with_sources(self, answer: str, sources: List[Dict]) -> str:
        """
        Format the final response with better readability and clear source attribution
        """
        if not answer:
            return "No response generated"
        
        # Extract source citations from the answer
        source_pattern = r'\[SOURCE-(\d+)\]'
        answer_with_refs = re.sub(source_pattern, r'^[\1]^', answer)
        
        # Replace the placeholder with superscript numbers for cleaner citations
        # This keeps the citations but makes them less intrusive
        for i in range(1, 20):  # Assuming we won't have more than 20 sources
            answer_with_refs = answer_with_refs.replace(f'^[{i}]^', f'[{i}]')
        
        # Start with the processed answer
        formatted_response = answer_with_refs
        
        # Check if we have sources to add
        if sources:
            # Add a clear separator and sources section
            formatted_response += "\n\n---\n\n"
            formatted_response += "**Sources:**\n"
            
            source_entries = []
            for i, source in enumerate(sources):
                url = source.get('url', 'No URL available')
                title = source.get('title', 'Unnamed Source')
                source_id = source.get('id', '').replace('[', '').replace(']', '')
                
                # Extract just the number from SOURCE-X
                if '-' in source_id:
                    source_num = source_id.split('-')[1]
                else:
                    source_num = str(i+1)
                    
                # Format sources with the corresponding number from the text
                source_entries.append(f"[{source_num}] {url}")
            
            # Join all sources with line breaks
            formatted_response += "\n".join(source_entries)
        else:
            formatted_response += "\n\n---\n\n*No specific sources cited for this information.*"
        
        return formatted_response