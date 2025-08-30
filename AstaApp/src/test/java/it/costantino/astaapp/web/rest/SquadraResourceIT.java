package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.SquadraAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.repository.SquadraRepository;
import it.costantino.astaapp.service.dto.SquadraDTO;
import it.costantino.astaapp.service.mapper.SquadraMapper;
import jakarta.persistence.EntityManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for the {@link SquadraResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class SquadraResourceIT {

    private static final String DEFAULT_NOME = "AAAAAAAAAA";
    private static final String UPDATED_NOME = "BBBBBBBBBB";

    private static final String ENTITY_API_URL = "/api/squadras";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private SquadraRepository squadraRepository;

    @Autowired
    private SquadraMapper squadraMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restSquadraMockMvc;

    private Squadra squadra;

    private Squadra insertedSquadra;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Squadra createEntity() {
        return new Squadra().nome(DEFAULT_NOME);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Squadra createUpdatedEntity() {
        return new Squadra().nome(UPDATED_NOME);
    }

    @BeforeEach
    void initTest() {
        squadra = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedSquadra != null) {
            squadraRepository.delete(insertedSquadra);
            insertedSquadra = null;
        }
    }

    @Test
    @Transactional
    void createSquadra() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);
        var returnedSquadraDTO = om.readValue(
            restSquadraMockMvc
                .perform(
                    post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(squadraDTO))
                )
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            SquadraDTO.class
        );

        // Validate the Squadra in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedSquadra = squadraMapper.toEntity(returnedSquadraDTO);
        assertSquadraUpdatableFieldsEquals(returnedSquadra, getPersistedSquadra(returnedSquadra));

        insertedSquadra = returnedSquadra;
    }

    @Test
    @Transactional
    void createSquadraWithExistingId() throws Exception {
        // Create the Squadra with an existing ID
        squadra.setId(1L);
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restSquadraMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(squadraDTO)))
            .andExpect(status().isBadRequest());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void checkNomeIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        squadra.setNome(null);

        // Create the Squadra, which fails.
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        restSquadraMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(squadraDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void getAllSquadras() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        // Get all the squadraList
        restSquadraMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(squadra.getId().intValue())))
            .andExpect(jsonPath("$.[*].nome").value(hasItem(DEFAULT_NOME)));
    }

    @Test
    @Transactional
    void getSquadra() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        // Get the squadra
        restSquadraMockMvc
            .perform(get(ENTITY_API_URL_ID, squadra.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(squadra.getId().intValue()))
            .andExpect(jsonPath("$.nome").value(DEFAULT_NOME));
    }

    @Test
    @Transactional
    void getNonExistingSquadra() throws Exception {
        // Get the squadra
        restSquadraMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingSquadra() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the squadra
        Squadra updatedSquadra = squadraRepository.findById(squadra.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedSquadra are not directly saved in db
        em.detach(updatedSquadra);
        updatedSquadra.nome(UPDATED_NOME);
        SquadraDTO squadraDTO = squadraMapper.toDto(updatedSquadra);

        restSquadraMockMvc
            .perform(
                put(ENTITY_API_URL_ID, squadraDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isOk());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedSquadraToMatchAllProperties(updatedSquadra);
    }

    @Test
    @Transactional
    void putNonExistingSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(
                put(ENTITY_API_URL_ID, squadraDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(squadraDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateSquadraWithPatch() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the squadra using partial update
        Squadra partialUpdatedSquadra = new Squadra();
        partialUpdatedSquadra.setId(squadra.getId());

        restSquadraMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedSquadra.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedSquadra))
            )
            .andExpect(status().isOk());

        // Validate the Squadra in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertSquadraUpdatableFieldsEquals(createUpdateProxyForBean(partialUpdatedSquadra, squadra), getPersistedSquadra(squadra));
    }

    @Test
    @Transactional
    void fullUpdateSquadraWithPatch() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the squadra using partial update
        Squadra partialUpdatedSquadra = new Squadra();
        partialUpdatedSquadra.setId(squadra.getId());

        partialUpdatedSquadra.nome(UPDATED_NOME);

        restSquadraMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedSquadra.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedSquadra))
            )
            .andExpect(status().isOk());

        // Validate the Squadra in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertSquadraUpdatableFieldsEquals(partialUpdatedSquadra, getPersistedSquadra(partialUpdatedSquadra));
    }

    @Test
    @Transactional
    void patchNonExistingSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, squadraDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamSquadra() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        squadra.setId(longCount.incrementAndGet());

        // Create the Squadra
        SquadraDTO squadraDTO = squadraMapper.toDto(squadra);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restSquadraMockMvc
            .perform(
                patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(squadraDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the Squadra in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteSquadra() throws Exception {
        // Initialize the database
        insertedSquadra = squadraRepository.saveAndFlush(squadra);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the squadra
        restSquadraMockMvc
            .perform(delete(ENTITY_API_URL_ID, squadra.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return squadraRepository.count();
    }

    protected void assertIncrementedRepositoryCount(long countBefore) {
        assertThat(countBefore + 1).isEqualTo(getRepositoryCount());
    }

    protected void assertDecrementedRepositoryCount(long countBefore) {
        assertThat(countBefore - 1).isEqualTo(getRepositoryCount());
    }

    protected void assertSameRepositoryCount(long countBefore) {
        assertThat(countBefore).isEqualTo(getRepositoryCount());
    }

    protected Squadra getPersistedSquadra(Squadra squadra) {
        return squadraRepository.findById(squadra.getId()).orElseThrow();
    }

    protected void assertPersistedSquadraToMatchAllProperties(Squadra expectedSquadra) {
        assertSquadraAllPropertiesEquals(expectedSquadra, getPersistedSquadra(expectedSquadra));
    }

    protected void assertPersistedSquadraToMatchUpdatableProperties(Squadra expectedSquadra) {
        assertSquadraAllUpdatablePropertiesEquals(expectedSquadra, getPersistedSquadra(expectedSquadra));
    }
}
